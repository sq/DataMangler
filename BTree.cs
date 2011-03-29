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
        public enum WriteModes {
            Invalid,
            AppendDataAndKey, // Append new data and new key
            ReplaceData,      // Write new data over existing data
            AppendData,       // Append new data, erase existing data
        }

        public const uint CurrentFormatVersion = 4;
        public const int MaxSerializationBufferSize = 1024 * 64;

        public readonly StreamSource Storage;
        public readonly string Prefix;

        public readonly StreamRef IndexStream;
        public readonly StreamRef KeyStream;
        public readonly StreamRef DataStream;

        private bool _IsDisposed;
        private StreamRange _HeaderRange;
        private readonly GetKeyOfEntryFunc _GetKeyOfEntry;
        private MemoryStream _SerializationBuffer;

        public BTree (StreamSource storage, string prefix) {
            Storage = storage;
            Prefix = prefix;

            IndexStream = Storage.Open(prefix + "index");
            KeyStream = Storage.Open(prefix + "keys");
            DataStream = Storage.Open(prefix + "data");

            VersionCheck(IndexStream);
            VersionCheck(KeyStream);
            VersionCheck(DataStream);

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

        public StreamRange AccessNode (long index, MemoryMappedFileAccess access) {
            unchecked {
                long position = BTreeHeader.Size + (index * BTreeNode.TotalSize);
                return IndexStream.AccessRange(position, BTreeNode.TotalSize, access);
            }
        }

        public StreamRange AccessValue (long nodeIndex, uint valueIndex, MemoryMappedFileAccess access) {
            unchecked {
                long position = BTreeHeader.Size + ((nodeIndex * BTreeNode.TotalSize) + BTreeNode.OffsetOfValues + (valueIndex * BTreeValue.Size));
                return IndexStream.AccessRange(position, BTreeValue.Size, access);
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
        /// <param name="valueIndex">Contains the index of the value within the node that matched the key, if a match was found.</param>
        /// <returns>True if the key was found within the tree. False if the key was not found.</returns>
        public bool FindKey (TangleKey key, out long nodeIndex, out uint valueIndex) {
            long temp;
            uint temp2;

            return FindKey(key, false, out nodeIndex, out valueIndex, out temp, out temp2);
        }

        /// <summary>
        /// Searches the BTree for a provided key.
        /// </summary>
        /// <param name="key">The key to search for.</param>
        /// <param name="nodeIndex">Contains the index of the BTree node where the search ended.</param>
        /// <param name="valueIndex">Contains the index of the value within the node that matched the key, if a match was found. If a match was not found, contains the index of the leaf where the key should be inserted.</param>
        /// <param name="parentNodeIndex">Contains the index of the BTree node containing the node where the search ended.</param>
        /// <param name="parentValueIndex">Contains the index of the leaf within the parent BTree node that led to the BTree node where the search ended.</param>
        /// <returns>True if the key was found within the tree. False if the key was not found.</returns>
        public bool FindKey (TangleKey key, bool forInsertion, out long nodeIndex, out uint valueIndex, out long parentNodeIndex, out uint parentValueIndex) {
            uint keyLength = (uint)key.Data.Count;

            long nodeCount = NodeCount;
            long rootIndex = RootIndex;
            parentNodeIndex = rootIndex;
            parentValueIndex = 0;
            long currentNode = parentNodeIndex;

            fixed (byte* pKey = &key.Data.Array[key.Data.Offset])
                while (currentNode >= 0 && currentNode < nodeCount)
                    using (var range = AccessNode(currentNode, MemoryMappedFileAccess.Read)) {
                        var pNode = (BTreeNode*)range.Pointer;
                        if (pNode->IsValid == 0)
                            throw new InvalidDataException();

                        // As we descend the tree, we split any full nodes we encounter so that
                        //  if we end up performing an insertion, we won't need to then walk all the
                        //  way back *up* the tree and split in reverse.
                        if (forInsertion && (pNode->NumValues == BTreeNode.MaxValues)) {
                            if (parentNodeIndex == currentNode) {
                                // Splitting the root.
                                var newRootIndex = CreateRoot();

                                using (var newRootRange = AccessNode(newRootIndex, MemoryMappedFileAccess.ReadWrite)) {
                                    var pNewRoot = (BTreeNode*)newRootRange.Pointer;
                                    var pNewLeaves = (BTreeLeaf*)(newRootRange.Pointer + BTreeNode.OffsetOfLeaves);

                                    // This looks wrong, but it's not: We want the new root to contain 0 values,
                                    //  but have one leaf pointing to the old root, so that we can split the old
                                    //  root in half. Splitting will move a single value up into the new root.
                                    pNewRoot->HasLeaves = 1;
                                    pNewLeaves[0].NodeIndex = (uint)currentNode;
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
            var range = AccessNode(nodeIndex, MemoryMappedFileAccess.ReadWrite);

            var pNode = (BTreeNode*)range.Pointer;
            var pValues = (BTreeValue*)(range.Pointer + BTreeNode.OffsetOfValues);
            var pLeaves = (BTreeLeaf*)(range.Pointer + BTreeNode.OffsetOfLeaves);

            if (pNode->IsValid == 0)
                throw new InvalidDataException();
            if (pNode->NumValues >= BTreeNode.MaxValues)
                throw new InvalidDataException();

            pNode->IsValid = 0;

            if (valueIndex < pNode->NumValues) {
                var destPtr = (byte*)(&pValues[valueIndex + 1]);
                var copyLength = (byte*)pLeaves - destPtr;
                if (copyLength < 0)
                    throw new InvalidDataException();

                // Move values
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

                // Move leaves
                Native.memmove(
                    destPtr, (byte*)(&pLeaves[valueIndex + 1]),
                    new UIntPtr((uint)copyLength)
                );
            }

            pNode->NumValues += 1;

            return range;
        }

        /// <summary>
        /// Inserts a new value and leaf into the specified BTree node at the specified position. Existing value(s) and leaves are moved.
        /// </summary>
        /// <param name="nodeIndex">The index of the node to insert into.</param>
        /// <param name="valueIndex">The index of the value to move forward.</param>
        /// <param name="value">The BTreeValue containing information on the value. It must be fully filled in.</param>
        /// <param name="leaf">The leaf associated with the new value.</param>
        public void Insert (long nodeIndex, uint valueIndex, ref BTreeValue value, ref BTreeLeaf leaf) {
            using (var range = PrepareForInsert(nodeIndex, valueIndex)) {
                var pNode = (BTreeNode*)range.Pointer;
                var pValues = (BTreeValue*)(range.Pointer + BTreeNode.OffsetOfValues);
                var pLeaves = (BTreeLeaf*)(range.Pointer + BTreeNode.OffsetOfLeaves);

                if (pNode->IsValid != 0)
                    throw new InvalidDataException();

                pValues[valueIndex] = value;
                pLeaves[valueIndex + 1] = leaf;

                pNode->IsValid = 1;
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

            using (var leafRange = AccessNode(leafNodeIndex, MemoryMappedFileAccess.ReadWrite))
            using (var newRange = AccessNode(newIndex, MemoryMappedFileAccess.ReadWrite)) {
                var pLeaf = (BTreeNode*)leafRange.Pointer;
                var pLeafValues = (BTreeValue*)(leafRange.Pointer + BTreeNode.OffsetOfValues);
                var pLeafLeaves = (BTreeLeaf*)(leafRange.Pointer + BTreeNode.OffsetOfLeaves);

                var pNew = (BTreeNode*)newRange.Pointer;
                var pNewValues = (BTreeValue*)(newRange.Pointer + BTreeNode.OffsetOfValues);
                var pNewLeaves = (BTreeLeaf*)(newRange.Pointer + BTreeNode.OffsetOfLeaves);

                if (pLeaf->IsValid != 1)
                    throw new InvalidDataException();
                if (pLeaf->NumValues != BTreeNode.MaxValues)
                    throw new InvalidDataException();

                pLeaf->IsValid = 0;

                *pNew = new BTreeNode {
                    NumValues = (ushort)tMinus1,
                    HasLeaves = pLeaf->HasLeaves,
                    IsValid = 0
                };

                pLeaf->NumValues = (ushort)tMinus1;

                Native.memmove((byte*)pNewValues, (byte*)&pLeafValues[BTreeNode.T], new UIntPtr(tMinus1 * BTreeValue.Size));

                if (pLeaf->HasLeaves == 1)
                    Native.memmove((byte*)pNewLeaves, (byte*)&pLeafLeaves[BTreeNode.T], new UIntPtr(BTreeNode.T * BTreeLeaf.Size));

                pNew->IsValid = 1;
                pLeaf->IsValid = 1;

                var newLeaf = new BTreeLeaf(newIndex);
                Insert(
                    parentNodeIndex, leafValueIndex,
                    ref pLeafValues[tMinus1], ref newLeaf
                );
            }
        }

        /// <summary>
        /// Allocates space for a new BTree node.
        /// </summary>
        /// <returns>The index of the new node.</returns>
        private long CreateNode () {
            long offset = IndexStream.AllocateSpace(BTreeNode.TotalSize).Value;
            var newIndex = (offset - BTreeHeader.Size) / BTreeNode.TotalSize;

            return newIndex;
        }

        private long CreateRoot () {
            return CreateRoot(RootIndex);
        }

        /// <summary>
        /// Creates a new BTree root node, replacing the old root node.
        /// </summary>
        /// <param name="oldRootIndex">The index of the old root node.</param>
        /// <returns>The index of the new root node.</returns>
        private long CreateRoot (long oldRootIndex) {
            var newIndex = CreateNode();

            using (var range = AccessNode(newIndex, MemoryMappedFileAccess.Write)) {
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

        public void ReadData<T> (ref BTreeValue entry, Deserializer<T> deserializer, out T value) {
            if (entry.KeyType == 0)
                throw new InvalidDataException();

            fixed (BTreeValue* pEntry = &entry)
                using (var range = DataStream.AccessRange(entry.DataOffset, entry.DataLength)) {
                    var context = new DeserializationContext(_GetKeyOfEntry, pEntry, range.Pointer, entry.DataLength);
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

        // BTreeValue must be fully prepared for the write operation:
        //  KeyOffset/KeyLength must be filled in.
        //  DataOffset/DataLength must be filled in.
        //  The BTreeValue's IsValid must be 0.
        // newOffset must specify the offset within the data stream where the data is to be written.
        //  In most cases this should be equal to DataOffset, but in the case of AppendData it will be different.
        public void WriteData (ref BTreeValue btreeValue, ref ArraySegment<byte> data, WriteModes writeMode, long? dataOffset) {
            if (btreeValue.KeyType != 0)
                throw new InvalidDataException();
            if (writeMode == WriteModes.Invalid)
                throw new InvalidDataException();

            if (!dataOffset.HasValue) {
                if (data.Count == 0)
                    return;
                else
                    throw new InvalidDataException();
            }

            var count = (uint)data.Count;
            var pHeader = (BTreeHeader*)_HeaderRange.Pointer;

            if (writeMode == WriteModes.AppendData) {
                using (var range = DataStream.AccessRange(btreeValue.DataOffset, btreeValue.DataLength, MemoryMappedFileAccess.Write))
                    Unsafe.ZeroBytes(range.Pointer, 0, btreeValue.DataLength);

                Interlocked.Add(ref pHeader->WastedDataBytes, btreeValue.DataLength + btreeValue.ExtraDataBytes);

                btreeValue.DataOffset = (uint)dataOffset.Value;
                btreeValue.DataLength = count;
                btreeValue.ExtraDataBytes = 0;
            } else if (writeMode != WriteModes.ReplaceData) {
                if (dataOffset.HasValue)
                    btreeValue.DataOffset = (uint)dataOffset.Value;

                btreeValue.DataLength = count;
                btreeValue.ExtraDataBytes = 0;
            }

            using (var range = DataStream.AccessRange(btreeValue.DataOffset, btreeValue.DataLength + btreeValue.ExtraDataBytes, MemoryMappedFileAccess.Write)) {
                Unsafe.WriteBytes(range.Pointer, 0, data);

                if (writeMode == WriteModes.ReplaceData) {
                    var bytesToZero = (btreeValue.DataLength + btreeValue.ExtraDataBytes) - count;
                    if (bytesToZero > 0)
                        Unsafe.ZeroBytes(range.Pointer, count, bytesToZero);

                    btreeValue.ExtraDataBytes += bytesToZero;
                    btreeValue.DataLength = count;
                }
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

        public long ValueCount {
            get {
                var pHeader = (BTreeHeader*)_HeaderRange.Pointer;
                return pHeader->ItemCount;
            }
        }

        public long AdjustValueCount (long delta) {
            var pHeader = (BTreeHeader*)_HeaderRange.Pointer;
            return Interlocked.Add(ref pHeader->ItemCount, delta);
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
        }
    }
}
