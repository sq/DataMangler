using System;
using System.Collections.Generic;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Linq;
using System.Text;
using System.Threading;
using Squared.Data.Mangler.Serialization;

namespace Squared.Data.Mangler.Internal {
    internal unsafe class BTree : IDisposable {
        public const uint CurrentFormatVersion = 4;
        public const int MaxSerializationBufferSize = 1024 * 64;

        public readonly StreamSource Storage;
        public readonly string Prefix;

        public readonly StreamRef IndexStream;
        public readonly StreamRef KeyStream;
        public readonly StreamRef DataStream;
        public readonly StreamRef FreelistStream;

        private StreamRange _HeaderRange;
        private readonly GetKeyOfEntryFunc _GetKeyOfEntry;
        private MemoryStream _SerializationBuffer;

        public BTree (StreamSource storage, string prefix) {
            Storage = storage;
            Prefix = prefix;

            IndexStream = Storage.Open(prefix + "index");
            KeyStream = Storage.Open(prefix + "keys");
            DataStream = Storage.Open(prefix + "data");
            FreelistStream = Storage.Open(prefix + "freelist");

            VersionCheck(IndexStream);
            VersionCheck(KeyStream);
            VersionCheck(DataStream);
            VersionCheck(FreelistStream);

            bool needInit = IndexStream.Length < BTreeHeader.Size;

            IndexStream.LengthChanging += IndexStream_LengthChanging;
            IndexStream.LengthChanged += IndexStream_LengthChanged;

            _GetKeyOfEntry = ReadKey;

            _HeaderRange = IndexStream.AccessRangeUncached(0, BTreeHeader.Size);

            if (needInit)
                InitializeBTree();
        }

        void IndexStream_LengthChanging (object sender, EventArgs e) {
            _HeaderRange.Dispose();
        }

        void IndexStream_LengthChanged (object sender, EventArgs e) {
            _HeaderRange = IndexStream.AccessRangeUncached(0, BTreeHeader.Size);
        }

        private static void VersionCheck (StreamRef stream) {
            var streamVersion = stream.FormatVersion;
            if (streamVersion == 0) {
                stream.FormatVersion = CurrentFormatVersion;
            } else if (streamVersion != CurrentFormatVersion) {
                throw new NotImplementedException("Format upgrade not implemented");
            }
        }

        public StreamRange AccessNode (long index, bool forWrite) {
            unchecked {
                long position = BTreeHeader.Size + (index * BTreeNode.TotalSize);
                var range = IndexStream.AccessRange(position, BTreeNode.TotalSize, MemoryMappedFileAccess.ReadWrite);

                var pNode = (BTreeNode*)range.Pointer;
                if (pNode->IsValid != 1) {
                    // If the node is fully uninitialized, locking it for write is okay
                    if ((pNode->NumValues != 0) || (pNode->HasLeaves != 0))
                        throw new InvalidDataException();
                }

                if (forWrite)
                    pNode->IsValid = 0;

                return range;
            }
        }

        public void UnlockNode (StreamRange range) {
            var pNode = (BTreeNode*)range.Pointer;
            if (pNode->IsValid != 0)
                throw new InvalidDataException();

            pNode->IsValid = 1;
        }

        public StreamRange AccessValue (long nodeIndex, uint valueIndex) {
            unchecked {
                long position = BTreeHeader.Size + ((nodeIndex * BTreeNode.TotalSize) + BTreeNode.OffsetOfValues + (valueIndex * BTreeValue.Size));
                return IndexStream.AccessRange(position, BTreeValue.Size, MemoryMappedFileAccess.ReadWrite);
            }
        }

        public BTreeValue * LockValue (StreamRange nodeRange, long valueIndex, ushort keyType) {
            unchecked {
                var pEntry = (BTreeValue*)(nodeRange.Pointer + BTreeNode.OffsetOfValues + (valueIndex * BTreeValue.Size));

                if (pEntry->KeyType != keyType)
                    throw new InvalidDataException();

                pEntry->KeyType = 0;

                return pEntry;
            }
        }

        public BTreeValue* LockValue (StreamRange nodeRange, long valueIndex, out ushort keyType) {
            unchecked {
                var pEntry = (BTreeValue*)(nodeRange.Pointer + BTreeNode.OffsetOfValues + (valueIndex * BTreeValue.Size));

                if (pEntry->KeyType == 0)
                    throw new InvalidDataException();

                keyType = pEntry->KeyType;
                pEntry->KeyType = 0;

                return pEntry;
            }
        }

        public void UnlockValue (BTreeValue * pEntry, ushort keyType) {
            unchecked {
                if (pEntry->KeyType != 0)
                    throw new InvalidDataException();

                pEntry->KeyType = keyType;
            }
        }

        /// <summary>
        /// Performs a binary search of the values stored within a BTree node.
        /// </summary>
        /// <param name="entries">Pointer to the first value within the node.</param>
        /// <param name="entryCount">The number of values within the node.</param>
        /// <param name="pSearchKey">Pointer to the first byte of the key to search for.</param>
        /// <param name="searchKeyLength">The number of bytes in the key to search for.</param>
        /// <param name="resultIndex">The index of the value within the node that matches the provided key, if any, otherwise the index of the leaf that should contain the provided key.</param>
        /// <returns>True if one of the node's values matches the provided key. False if the provided key was not found.</returns>
        private bool SearchValues (BTreeValue* entries, ushort entryCount, byte* pSearchKey, uint searchKeyLength, out uint resultIndex) {
            int min = 0, max = entryCount - 1;
            int delta = 0, pivot;
            uint compareLength;

            while (min <= max) {
                unchecked {
                    pivot = min + ((max - min) >> 1);
                }

                var pEntry = &entries[pivot];
                if (pEntry->KeyType == 0)
                    throw new InvalidDataException();

                compareLength = Math.Min(Math.Min(pEntry->KeyLength, searchKeyLength), BTreeValue.KeyPrefixSize);

                // Calling out to a function here introduces unnecessary overhead since the prefix is so small
                for (uint i = 0; i < compareLength; i++) {
                    delta = pEntry->KeyPrefix[i] - pSearchKey[i];
                    if (delta != 0)
                        break;
                }

                if ((delta == 0) && (pEntry->KeyLength > BTreeValue.KeyPrefixSize))
                    using (var keyRange = KeyStream.AccessRange(
                        pEntry->KeyOffset, pEntry->KeyLength, MemoryMappedFileAccess.Read
                    )) {
                        var pLhs = keyRange.Pointer;

                        compareLength = Math.Min(pEntry->KeyLength, searchKeyLength);
                        delta = Native.memcmp(pLhs, pSearchKey, new UIntPtr(compareLength));
                    }

                if (delta == 0) {
                    if (pEntry->KeyLength > searchKeyLength)
                        delta = 1;
                    else if (searchKeyLength > pEntry->KeyLength)
                        delta = -1;
                }

                if (delta == 0) {
                    resultIndex = (uint)pivot;
                    return true;
                } else if (delta < 0) {
                    min = pivot + 1;
                } else {
                    max = pivot - 1;
                }
            }

            resultIndex = (uint)min;
            return false;
        }

        /// <summary>
        /// Searches the BTree for a provided key.
        /// </summary>
        /// <param name="key">The key to search for.</param>
        /// <param name="nodeIndex">Contains the index of the BTree node where the search ended.</param>
        /// <param name="valueIndex">Contains the index of the value within the node that matched the key, if a match was found. If a match was not found, contains the index of the leaf where the key should be inserted.</param>
        /// <returns>True if the key was found within the tree. False if the key was not found.</returns>
        public bool FindKey (TangleKey key, bool forInsertion, out long nodeIndex, out uint valueIndex) {
            uint keyLength = (uint)key.Data.Count;

            long nodeCount = NodeCount;
            long rootIndex = RootIndex;
            long parentNodeIndex = rootIndex;
            long currentNode = parentNodeIndex;
            uint parentValueIndex = 0;

            fixed (byte* pKey = &key.Data.Array[key.Data.Offset])
            while (currentNode >= 0 && currentNode < nodeCount)
            using (var range = AccessNode(currentNode, false)) {
                var pNode = (BTreeNode*)range.Pointer;

                // As we descend the tree, we split any full nodes we encounter so that
                //  if we end up performing an insertion, we won't need to then walk all the
                //  way back *up* the tree and split in reverse.
                if (forInsertion && (pNode->NumValues == BTreeNode.MaxValues)) {
                    if (parentNodeIndex == currentNode) {
                        // Splitting the root.
                        var newRootIndex = CreateRoot();

                        using (var newRootRange = AccessNode(newRootIndex, true)) {
                            var pNewRoot = (BTreeNode*)newRootRange.Pointer;
                            var pNewLeaves = (BTreeLeaf*)(newRootRange.Pointer + BTreeNode.OffsetOfLeaves);

                            // This looks wrong, but it's not: We want the new root to contain 0 values,
                            //  but have one leaf pointing to the old root, so that we can split the old
                            //  root in half. Splitting will move a single value up into the new root.
                            pNewRoot->HasLeaves = 1;
                            pNewLeaves[0].NodeIndex = (uint)currentNode;

                            UnlockNode(newRootRange);
                        }

                        SplitLeafNode(newRootIndex, 0, currentNode);

                        // Restart at the root
                        nodeCount += 2;
                        currentNode = parentNodeIndex = rootIndex = newRootIndex;
                        parentValueIndex = 0;
                        continue;
                    } else {
                        // Splitting a regular node.
                        SplitLeafNode(parentNodeIndex, parentValueIndex, currentNode);

                        // Restart at the root
                        nodeCount += 1;
                        currentNode = parentNodeIndex = rootIndex;
                        parentValueIndex = 0;
                        continue;
                    }
                }

                var pValues = (BTreeValue*)(range.Pointer + BTreeNode.OffsetOfValues);
                if (SearchValues(pValues, pNode->NumValues, pKey, keyLength, out valueIndex)) {
                    // Found an exact match within the BTree node.
                    nodeIndex = currentNode;
                    return true;
                }

                if (pNode->HasLeaves == 1) {
                    // No exact match was found, so valueIndex now contains the index of the leaf node.
                    var pLeaves = (BTreeLeaf*)(range.Pointer + BTreeNode.OffsetOfLeaves);

                    parentNodeIndex = currentNode;
                    parentValueIndex = valueIndex;

                    currentNode = pLeaves[valueIndex].NodeIndex;
                } else {
                    // The value was not found inside the node, and we're in a node with no leaves, so there is no match.
                    nodeIndex = currentNode;
                    return false;
                }
            }

            throw new InvalidDataException("Current node left the index");
        }

        /// <summary>
        /// Prepares a BTree node for the insertion of a value by moving existing values/leaves out of the way.
        /// The node is flagged as invalid and its value count is incremented.
        /// </summary>
        /// <param name="nodeIndex">The index of the node.</param>
        /// <param name="valueIndex">The index of the value within the node that will be moved forward to make room for the new value.</param>
        /// <returns>A StreamRange that can be used to access the specified BTree node.</returns>
        public StreamRange PrepareForInsert (long nodeIndex, uint valueIndex) {
            var range = AccessNode(nodeIndex, true);

            var pNode = (BTreeNode*)range.Pointer;
            var pValues = (BTreeValue*)(range.Pointer + BTreeNode.OffsetOfValues);
            var pLeaves = (BTreeLeaf*)(range.Pointer + BTreeNode.OffsetOfLeaves);

            if (pNode->NumValues >= BTreeNode.MaxValues)
                throw new InvalidDataException();

            if (valueIndex < pNode->NumValues) {
                var destPtr = (byte*)(&pValues[valueIndex + 1]);
                var copyLength = (byte*)pLeaves - destPtr;
                if (copyLength < 0)
                    throw new InvalidDataException();

                Native.memmove(
                    destPtr, (byte*)(&pValues[valueIndex]),
                    new UIntPtr((uint)copyLength)
                );
            }

            if (pNode->HasLeaves == 1) {
                var pEnd = (byte*)range.Pointer + BTreeNode.TotalSize;
                var destPtr = (byte*)(&pLeaves[valueIndex + 2]);
                var copyLength = (byte*)pEnd - destPtr;
                if (copyLength < 0)
                    throw new InvalidDataException();

                Native.memmove(
                    destPtr, (byte*)(&pLeaves[valueIndex + 1]),
                    new UIntPtr((uint)copyLength)
                );
            }

            // If we don't mark the now-empty spot as invalid, later attempts to lock it will fail
            pValues[valueIndex].KeyType = 0;

            pNode->NumValues += 1;

            return range;
        }

        public void FinalizeInsert (StreamRange range) {
            UnlockNode(range);

            var pHeader = (BTreeHeader*)_HeaderRange.Pointer;
            Interlocked.Increment(ref pHeader->ItemCount);
        }

        /// <summary>
        /// Inserts a new value and leaf into the specified BTree node at the specified position. Existing value(s) and leaves are moved.
        /// </summary>
        /// <param name="nodeIndex">The index of the node to insert into.</param>
        /// <param name="valueIndex">The index of the value to move forward.</param>
        /// <param name="value">The BTreeValue containing information on the value. It must be fully filled in.</param>
        /// <param name="leaf">The leaf associated with the new value.</param>
        private void Insert (long nodeIndex, uint valueIndex, ref BTreeValue value, ref BTreeLeaf leaf) {
            using (var range = PrepareForInsert(nodeIndex, valueIndex)) {
                var pNode = (BTreeNode*)range.Pointer;
                var pValues = (BTreeValue*)(range.Pointer + BTreeNode.OffsetOfValues);
                var pLeaves = (BTreeLeaf*)(range.Pointer + BTreeNode.OffsetOfLeaves);

                pValues[valueIndex] = value;
                pLeaves[valueIndex + 1] = leaf;

                UnlockNode(range);
            }
        }

        /// <summary>
        /// Splits a BTree leaf node into two nodes. The median value from the node moves upward into the parent, with the two new nodes each recieving half of the remaining values and leaves.
        /// </summary>
        /// <param name="parentNodeIndex">The index of the node's parent node.</param>
        /// <param name="leafValueIndex">The index within the parent node's values where the median value should be inserted.</param>
        /// <param name="leafNodeIndex">The index of the leaf node to split.</param>
        public void SplitLeafNode (long parentNodeIndex, uint leafValueIndex, long leafNodeIndex) {
            const int tMinus1 = BTreeNode.T - 1;

            long newIndex = CreateNode();

            using (var leafRange = AccessNode(leafNodeIndex, true))
            using (var newRange = AccessNode(newIndex, true)) {
                var pLeaf = (BTreeNode*)leafRange.Pointer;
                var pLeafValues = (BTreeValue*)(leafRange.Pointer + BTreeNode.OffsetOfValues);
                var pLeafLeaves = (BTreeLeaf*)(leafRange.Pointer + BTreeNode.OffsetOfLeaves);

                var pNew = (BTreeNode*)newRange.Pointer;
                var pNewValues = (BTreeValue*)(newRange.Pointer + BTreeNode.OffsetOfValues);
                var pNewLeaves = (BTreeLeaf*)(newRange.Pointer + BTreeNode.OffsetOfLeaves);

                if (pLeaf->NumValues != BTreeNode.MaxValues)
                    throw new InvalidDataException();

                *pNew = new BTreeNode {
                    NumValues = (ushort)tMinus1,
                    HasLeaves = pLeaf->HasLeaves,
                    IsValid = 0
                };

                pLeaf->NumValues = (ushort)tMinus1;

                Native.memmove((byte*)pNewValues, (byte*)&pLeafValues[BTreeNode.T], new UIntPtr(tMinus1 * BTreeValue.Size));

                if (pLeaf->HasLeaves == 1)
                    Native.memmove((byte*)pNewLeaves, (byte*)&pLeafLeaves[BTreeNode.T], new UIntPtr(BTreeNode.T * BTreeLeaf.Size));

                var newLeaf = new BTreeLeaf(newIndex);
                Insert(
                    parentNodeIndex, leafValueIndex,
                    ref pLeafValues[tMinus1], ref newLeaf
                );

                UnlockNode(newRange);
                UnlockNode(leafRange);
            }
        }

        private long CreateNode () {
            long offset = IndexStream.AllocateSpace(BTreeNode.TotalSize).Value;
            var newIndex = (offset - BTreeHeader.Size) / BTreeNode.TotalSize;

            return newIndex;
        }

        private long CreateRoot () {
            return CreateRoot(RootIndex);
        }

        private long CreateRoot (long oldRootIndex) {
            var newIndex = CreateNode();

            using (var range = AccessNode(newIndex, true)) {
                var pNode = (BTreeNode*)range.Pointer;
                *pNode = new BTreeNode {
                    HasLeaves = 0,
                    IsValid = 1,
                    NumValues = 0
                };
            }

            var pHeader = (BTreeHeader*)_HeaderRange.Pointer;
            if (Interlocked.CompareExchange(ref pHeader->RootIndex, newIndex, oldRootIndex) != oldRootIndex)
                throw new InvalidDataException();

            return newIndex;
        }

        private void InitializeBTree () {
            IndexStream.AllocateSpace(BTreeHeader.Size);

            CreateRoot();
        }

        public bool ReadKey (BTreeValue* pEntry, out TangleKey key) {
            ushort keyType = pEntry->KeyType;
            return ReadKey(pEntry, keyType, out key);
        }

        public bool ReadKey (BTreeValue* pEntry, ushort keyType, out TangleKey key) {
            if (keyType == 0) {
                key = default(TangleKey);
                return false;
            }

            var buffer = ImmutableArrayPool<byte>.Allocate(pEntry->KeyLength);

            if (pEntry->KeyLength <= BTreeValue.KeyPrefixSize) {
                fixed (byte* pBuffer = buffer.Array)
                    Native.memmove(pBuffer + buffer.Offset, pEntry->KeyPrefix, new UIntPtr(pEntry->KeyLength));
            } else {
                using (var keyRange = KeyStream.AccessRange(
                    pEntry->KeyOffset, pEntry->KeyLength, MemoryMappedFileAccess.Read
                ))
                    Unsafe.ReadBytes(keyRange.Pointer, 0, buffer.Array, buffer.Offset, pEntry->KeyLength);
            }

            key = new TangleKey(buffer, keyType);
            return true;
        }

        public void WriteKey (ref BTreeValue btreeValue, TangleKey key) {
            var prefixSize = Math.Min(key.Data.Count, BTreeValue.KeyPrefixSize);

            fixed (byte* pPrefix = btreeValue.KeyPrefix)
                Unsafe.WriteBytes(pPrefix, 0, key.Data.Array, key.Data.Offset, prefixSize);

            if (prefixSize < key.Data.Count) {
                using (var keyRange = KeyStream.AccessRange(btreeValue.KeyOffset, btreeValue.KeyLength, MemoryMappedFileAccess.Write))
                    Unsafe.WriteBytes(keyRange.Pointer, 0, key.Data);
            }
        }

        public void WriteNewKey (BTreeValue * pEntry, TangleKey key) {
            if (key.Data.Count > BTreeValue.KeyPrefixSize)
                pEntry->KeyOffset = (uint)KeyStream.AllocateSpace((uint)key.Data.Count).Value;
            else
                pEntry->KeyOffset = 0;

            pEntry->KeyLength = (ushort)key.Data.Count;

            // It's important that we zero out these fields so that when we write the data,
            //  it's done in append mode instead of replace mode
            pEntry->DataOffset = 0;
            pEntry->DataLength = 0;
            pEntry->ExtraDataBytes = 0;

            WriteKey(ref *pEntry, key);
        }

        public void ReadData<T> (ref BTreeValue entry, Deserializer<T> deserializer, out T value) {
            var keyType = entry.KeyType;
            if (keyType == 0)
                throw new InvalidDataException();

            fixed (BTreeValue * pEntry = &entry)
                ReadData(pEntry, keyType, deserializer, out value);
        }

        public void ReadData<T> (BTreeValue * pEntry, Deserializer<T> deserializer, out T value) {
            var keyType = pEntry->KeyType;
            if (keyType == 0)
                throw new InvalidDataException();

            ReadData(pEntry, keyType, deserializer, out value);
        }

        public unsafe void ReadData<T> (BTreeValue * pEntry, ushort keyType, Deserializer<T> deserializer, out T value) {
            using (var range = DataStream.AccessRange(pEntry->DataOffset, pEntry->DataLength)) {
                var context = new DeserializationContext(_GetKeyOfEntry, pEntry, keyType, range.Pointer, pEntry->DataLength);
                try {
                    deserializer(ref context, out value);
                } finally {
                    context.Dispose();
                }
            }
        }

        /// <summary>
        /// Serializes a given value so that it can be written to the provided IndexEntry.
        /// The IndexEntry's KeyLength and KeyOffset values must be filled in and the key must have been written to KeyOffset.
        /// </summary>
        public ArraySegment<byte> Serialize<T> (BTreeValue* pEntry, Serializer<T> serializer, ushort keyType, ref T value) {
            if (_SerializationBuffer == null)
                _SerializationBuffer = new MemoryStream();

            var context = new SerializationContext(_GetKeyOfEntry, pEntry, keyType, _SerializationBuffer);
            serializer(ref context, ref value);
            var result = context.Bytes;

            if (_SerializationBuffer.Capacity > MaxSerializationBufferSize) {
                _SerializationBuffer.Dispose();
                _SerializationBuffer = null;
            } else {
                _SerializationBuffer.Seek(0, SeekOrigin.Begin);
                _SerializationBuffer.SetLength(0);
            }

            return result;
        }

        private unsafe long? FreelistGet (ref uint size) {
            long count = FreelistStream.Length / FreelistNode.Size;

            using (var range = FreelistStream.AccessRange(0, (uint)FreelistStream.Length, MemoryMappedFileAccess.ReadWrite))
            for (long i = 0; i < count; i++) {
                FreelistNode * pNode = (FreelistNode *)(range.Pointer + (i * FreelistNode.Size));

                if (pNode->BlockSize >= size) {
                    var offset = pNode->BlockOffset;
                    size = pNode->BlockSize;

                    Native.memmove((byte *)pNode, (range.Pointer + (FreelistStream.Length - FreelistNode.Size)), new UIntPtr(FreelistNode.Size));
                    FreelistStream.Shrink((int)FreelistNode.Size);

                    var pHeader = (BTreeHeader*)_HeaderRange.Pointer;
                    Interlocked.Add(ref pHeader->WastedDataBytes, -size);

                    return offset;
                }
            }

            return null;
        }

        private unsafe void FreelistPut (long blockOffset, uint blockSize) {
            long? offset = FreelistStream.AllocateSpace(FreelistNode.Size);

            using (var range = FreelistStream.AccessRange(offset.Value, FreelistNode.Size, MemoryMappedFileAccess.ReadWrite)) {
                FreelistNode* pNode = (FreelistNode *)range.Pointer;
                pNode->BlockOffset = (uint)blockOffset;
                pNode->BlockSize = blockSize;
            }

            var pHeader = (BTreeHeader*)_HeaderRange.Pointer;
            Interlocked.Add(ref pHeader->WastedDataBytes, blockSize);
        }

        private long? AllocateDataSpace (ref uint size) {
            size = ((size + 3) / 4) * 4;

            var result = FreelistGet(ref size);
            if (!result.HasValue)
                result = DataStream.AllocateSpace(size);

            return result;
        }

        public void WriteData (BTreeValue * pEntry, ArraySegment<byte> data) {
            bool append = (data.Count > pEntry->DataLength + pEntry->ExtraDataBytes);

            long? dataOffset;
            uint size = (uint)data.Count;
            if (append)
                dataOffset = AllocateDataSpace(ref size);
            else
                dataOffset = pEntry->DataOffset;

            WriteData(ref *pEntry, data, append, dataOffset, size);
        }

        // BTreeValue must be fully prepared for the write operation:
        //  KeyOffset/KeyLength must be filled in.
        //  DataOffset/DataLength must be filled in.
        //  The BTreeValue's IsValid must be 0.
        // newOffset must specify the offset within the data stream where the data is to be written.
        //  In most cases this should be equal to DataOffset, but in the case of AppendData it will be different.
        private void WriteData (ref BTreeValue btreeValue, ArraySegment<byte> data, bool append, long? dataOffset, uint size) {
            if (btreeValue.KeyType != 0)
                throw new InvalidDataException();

            if (!dataOffset.HasValue) {
                if (data.Count == 0)
                    return;
                else
                    throw new InvalidDataException();
            }

            var count = (uint)data.Count;
            uint bytesToZero = 0;

            if (append) {
                if (btreeValue.DataLength > 0) {
                    using (var range = DataStream.AccessRange(btreeValue.DataOffset, btreeValue.DataLength, MemoryMappedFileAccess.Write))
                        Unsafe.ZeroBytes(range.Pointer, 0, btreeValue.DataLength);

                    FreelistPut(btreeValue.DataOffset, btreeValue.DataLength);
                }

                btreeValue.DataOffset = (uint)dataOffset.Value;
                btreeValue.DataLength = count;
                btreeValue.ExtraDataBytes = size - count;
            } else {
                if (dataOffset.HasValue)
                    btreeValue.DataOffset = (uint)dataOffset.Value;

                bytesToZero = (btreeValue.DataLength + btreeValue.ExtraDataBytes) - count;
                btreeValue.DataLength = count;
                btreeValue.ExtraDataBytes = bytesToZero;
            }

            using (var range = DataStream.AccessRange(btreeValue.DataOffset, btreeValue.DataLength + btreeValue.ExtraDataBytes, MemoryMappedFileAccess.Write)) {
                if (count > 0)
                    Unsafe.WriteBytes(range.Pointer, 0, data);

                if (bytesToZero > 0)
                    Unsafe.ZeroBytes(range.Pointer, count, bytesToZero);
            }
        }

        public long RootIndex {
            get {
                var pHeader = (BTreeHeader*)_HeaderRange.Pointer;
                return pHeader->RootIndex;
            }
        }

        public long NodeCount {
            get {
                return (IndexStream.Length - BTreeHeader.Size) / BTreeNode.TotalSize;
            }
        }

        public long MutationSentinel {
            get {
                var pHeader = (BTreeHeader*)_HeaderRange.Pointer;
                return pHeader->MutationSentinel;
            }
            set {
                var pHeader = (BTreeHeader*)_HeaderRange.Pointer;
                pHeader->MutationSentinel = value;
            }
        }

        public long ValueCount {
            get {
                var pHeader = (BTreeHeader*)_HeaderRange.Pointer;
                return pHeader->ItemCount;
            }
        }

        public long WastedDataBytes {
            get {
                var pHeader = (BTreeHeader*)_HeaderRange.Pointer;
                return pHeader->WastedDataBytes;
            }
        }

        public void Dispose () {
            IndexStream.Dispose();
            KeyStream.Dispose();
            DataStream.Dispose();
        }
    }
}
