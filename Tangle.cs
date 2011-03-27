﻿/*
The contents of this file are subject to the Mozilla Public License
Version 1.1 (the "License"); you may not use this file except in
compliance with the License. You may obtain a copy of the License at
http://www.mozilla.org/MPL/

Software distributed under the License is distributed on an "AS IS"
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
License for the specific language governing rights and limitations
under the License.

The Original Code is DataMangler Key-Value Store.

The Initial Developer of the Original Code is Mozilla Corporation.

Original Author: Kevin Gadd (kevin.gadd@gmail.com)
*/

using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;
using System.Threading.Tasks;
using Squared.Data.Mangler.Serialization;
using Squared.Task;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading;
using Squared.Data.Mangler.Internal;
using System.IO.MemoryMappedFiles;
using System.Collections.Concurrent;
using TaskScheduler = Squared.Task.TaskScheduler;

namespace Squared.Data.Mangler {
    public class KeyNotFoundException : Exception {
        public readonly TangleKey Key;

        public KeyNotFoundException (TangleKey key) {
            Key = key;
        }

        public override string Message {
            get {
                return String.Format("The key '{0}' was not found.", Key);
            }
        }
    }

    public class SerializerThrewException : Exception {
        public readonly TangleKey Key;

        public SerializerThrewException (TangleKey key, Exception innerException)
            : base("", innerException) {
                Key = key;
        }

        public override string Message {
            get {
                return String.Format("The data for key '{0}' was not written because the serializer threw an exception.", Key);
            }
        }
    }

    /// <summary>
    /// Represents a persistent dictionary keyed by arbitrary byte strings. The values are not stored in any given order on disk, and the values are not required to be resident in memory.
    /// At any given time a portion of the Tangle's values may be resident in memory. If a value is not resident in memory, it will be fetched asynchronously from disk.
    /// The Tangle's keys are implicitly ordered, which allows for efficient lookups of individual values by key.
    /// Converting values to/from their disk format is handled by the provided TangleSerializer and TangleDeserializer.
    /// The Tangle's disk storage engine partitions its storage up into pages based on the provided page size. For optimal performance, this should be an integer multiple of the size of a memory page (typically 4KB).
    /// </summary>
    /// <typeparam name="T">The type of the value stored within the tangle.</typeparam>
    public unsafe partial class Tangle<T> : ITangle {
        enum WriteModes {
            Invalid,
            AppendDataAndKey, // Append new data and new key
            ReplaceData,      // Write new data over existing data
            AppendData,       // Append new data, erase existing data
        }

        public struct FindResult {
            public readonly Tangle<T> Tangle;
            public readonly TangleKey Key;
            private readonly long NodeIndex;
            private readonly uint ValueIndex;

            internal FindResult (Tangle<T> owner, TangleKey key, long nodeIndex, uint valueIndex) {
                Tangle = owner;
                Key = key;
                NodeIndex = nodeIndex;
                ValueIndex = valueIndex;
            }

            public Future<T> GetValue () {
                return Tangle.GetValueByIndex(NodeIndex, ValueIndex);
            }

            public IFuture SetValue (T newValue) {
                return Tangle.SetValueByIndex(NodeIndex, ValueIndex, ref newValue);
            }
        }


        public const bool TraceKeyInsertions = false;
        public const uint CurrentFormatVersion = 3;
        public const int MaxSerializationBufferSize = 1024 * 64;

        public static readonly int WorkerThreadTimeoutMs = 30000;

        public readonly bool OwnsStorage;
        public readonly StreamSource Storage;
        public readonly TaskScheduler Scheduler;
        public readonly Serializer<T> Serializer;
        public readonly Deserializer<T> Deserializer;

        protected readonly ReaderWriterLockSlim IndexLock = new ReaderWriterLockSlim(LockRecursionPolicy.NoRecursion);

        internal Squared.Task.Internal.WorkerThread<ConcurrentQueue<IWorkItem<T>>> _WorkerThread;

        private bool _IsDisposed;
        private MemoryStream _SerializationBuffer;
        private StreamRange _HeaderRange;
        private readonly GetKeyOfEntryFunc _GetKeyOfEntry;

        private readonly StreamRef IndexStream;
        private readonly StreamRef KeyStream;
        private readonly StreamRef DataStream;

        public Tangle (
            TaskScheduler scheduler, 
            StreamSource storage, 
            Serializer<T> serializer = null, 
            Deserializer<T> deserializer = null,
            bool ownsStorage = true
        ) {
            Scheduler = scheduler;
            Storage = storage;
            OwnsStorage = ownsStorage;

            Serializer = serializer ?? Defaults<T>.Serializer;
            Deserializer = deserializer ?? Defaults<T>.Deserializer;

            _GetKeyOfEntry = ReadKey;

            IndexStream = Storage.Open("index");
            KeyStream = Storage.Open("keys");
            DataStream = Storage.Open("data");

            VersionCheck(IndexStream);
            VersionCheck(KeyStream);
            VersionCheck(DataStream);

            bool needInit = IndexStream.Length < BTreeHeader.Size;

            IndexStream.LengthChanging += IndexStream_LengthChanging;
            IndexStream.LengthChanged += IndexStream_LengthChanged;

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

        /// <summary>
        /// Serializes a given value so that it can be written to the provided IndexEntry.
        /// The IndexEntry's KeyLength and KeyOffset values must be filled in and the key must have been written to KeyOffset.
        /// </summary>
        private ArraySegment<byte> Serialize (IndexEntry * pEntry, ushort keyType, ref T value) {
            if (_SerializationBuffer == null)
                _SerializationBuffer = new MemoryStream();

            var context = new SerializationContext(_GetKeyOfEntry, pEntry, keyType, _SerializationBuffer);
            Serializer(ref context, ref value);
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

        private static void VersionCheck (StreamRef stream) {
            var streamVersion = stream.FormatVersion;
            if (streamVersion == 0) {
                stream.FormatVersion = CurrentFormatVersion;
            } else if (streamVersion != CurrentFormatVersion) {
                throw new NotImplementedException("Format upgrade not implemented");
            }
        }

        IBarrier ITangle.CreateBarrier (bool createOpened) {
            return this.CreateBarrier(createOpened);
        }

        /// <summary>
        /// Inserts a barrier into the tangle's work queue. 
        /// A barrier prevents the execution of work items following it as long as it remains closed, and becomes signaled once that point in the queue is reached.
        /// </summary>
        /// <param name="createOpened">If true, the barrier is created open, which allows items following it in the work queue to be executed. Otherwise, the barrier is created closed (and can be opened manually.)</param>
        /// <returns>The barrier that was created.</returns>
        public Barrier CreateBarrier (bool createOpened = false) {
            return new Barrier(this, createOpened);
        }

        /// <summary>
        /// Creates a batch that can be used to write to tangles of this type.
        /// </summary>
        /// <param name="capacity">The maximum capacity of the batch.</param>
        /// <returns>A new batch instance.</returns>
        public static Batch<T> CreateBatch (int capacity) {
            return new Batch<T>(capacity);
        }

        /// <summary>
        /// Reads a value from the tangle, looking it up via its key.
        /// </summary>
        /// <returns>A future that will contain the value once it has been read.</returns>
        /// <exception cref="KeyNotFoundException">If the specified key is not found, the future will contain a KeyNotFoundException.</exception>
        public Future<T> Get (TangleKey key) {
            return QueueWorkItem(new GetThunk(key));
        }

        IFuture ITangle.Get (TangleKey key) {
            return Get(key);
        }

        /// <summary>
        /// Reads multiple values from the tangle, looking them up based on a provided sequence of keys.
        /// </summary>
        /// <param name="keys">The keys to look up in this tangle.</param>
        /// <returns>A future that will contain the retrieved values.</returns>
        public Future<KeyValuePair<TKey, T>[]> Select<TKey> (IEnumerable<TKey> keys) {
            return QueueWorkItem(new GetMultipleThunk<TKey>(keys));
        }

        /// <summary>
        /// Reads multiple values from the tangle, looking them up based on a provided sequence of keys,
        ///  and then uses those values to perform a lookup within a second tangle.
        /// </summary>
        /// <param name="keys">The keys to look up in this tangle.</param>
        /// <param name="right">The tangle to join against.</param>
        /// <param name="keySelector">A delegate that takes a key/value pair from this tangle and produces a key to use for a lookup in the other tangle.</param>
        /// <param name="valueSelector">A delegate that takes key/value pairs from both tangles and produces a result for the join.</param>
        /// <returns>A future that will contain the join results.</returns>
        public Future<TOut[]> Join<TLeftKey, TRightKey, TRight, TOut> (
            Tangle<TRight> right, IEnumerable<TLeftKey> keys,
            JoinKeySelector<TLeftKey, T, TRightKey> keySelector,
            JoinValueSelector<TLeftKey, T, TRightKey, TRight, TOut> valueSelector
        ) {
            var rightBarrier = new Tangle<TRight>.JoinBarrierThunk();
            right.QueueWorkItem(rightBarrier);
            return QueueWorkItem(new JoinThunk<TLeftKey, TRightKey, TRight, TOut>(
                rightBarrier, right, keys, keySelector, valueSelector
            ));
        }

        /// <summary>
        /// Reads multiple values from the tangle, looking them up based on a provided sequence of keys,
        ///  and then uses those values to perform a lookup within a second tangle.
        /// </summary>
        /// <param name="keys">The keys to look up in this tangle.</param>
        /// <param name="right">The tangle to join against.</param>
        /// <param name="keySelector">A delegate that takes a value from this tangle and produces a key to use for a lookup in the other tangle.</param>
        /// <returns>A future that will contain the join results.</returns>
        public Future<KeyValuePair<T, TRight>[]> Join<TLeftKey, TRightKey, TRight> (
            Tangle<TRight> right, IEnumerable<TLeftKey> keys,
            Func<T, TRightKey> keySelector
        ) {
            return Join(
                right, keys,
                (TLeftKey leftKey, ref T leftValue)
                    => keySelector(leftValue),
                (TLeftKey leftKey, ref T leftValue, TRightKey rightKey, ref TRight rightValue)
                    => new KeyValuePair<T, TRight>(leftValue, rightValue)
            );
        }

        /// <summary>
        /// Reads every key from the tangle, in no particular order.
        /// </summary>
        /// <returns>A future that will contain the retrieved keys.</returns>
        public Future<TangleKey[]> GetAllKeys () {
            return QueueWorkItem(new GetAllKeysThunk());
        }

        /// <summary>
        /// Reads every value from the tangle, in no particular order.
        /// </summary>
        /// <returns>A future that will contain the retrieved values.</returns>
        public Future<T[]> GetAllValues () {
            return QueueWorkItem(new GetAllValuesThunk());
        }

        IFuture ITangle.GetAllValues () {
            return GetAllValues();
        }

        protected Future<T> GetValueByIndex (long nodeIndex, uint valueIndex) {
            return QueueWorkItem(new GetByIndexThunk(nodeIndex, valueIndex));
        }

        protected IFuture SetValueByIndex (long nodeIndex, uint valueIndex, ref T value) {
            return QueueWorkItem(new SetByIndexThunk(nodeIndex, valueIndex, ref value));
        }

        /// <summary>
        /// Searches the tangle for a given key, and if it is found, returns a reference to the key that can be used to retrieve or replace its associated value.
        /// </summary>
        /// <returns>A future that will contain a reference to the key, if it was found.</returns>
        /// <exception cref="KeyNotFoundException">If the specified key is not found, the future will contain a KeyNotFoundException.</exception>
        public Future<FindResult> Find (TangleKey key) {
            return QueueWorkItem(new FindThunk(key));
        }

        /// <summary>
        /// Stores a value into the tangle, assigning it a given key. If the given key already has an associated value, that value is replaced.
        /// </summary>
        /// <returns>A future that completes once the value has been stored to disk.</returns>
        public IFuture Set (TangleKey key, T value) {
            return QueueWorkItem(new SetThunk(key, ref value, true));
        }

        /// <summary>
        /// Stores a value into the tangle, assigning it a given key. If the given key already has an associated value, the operation will abort.
        /// </summary>
        /// <returns>A future that completes once the value has been stored to disk. The future's value will be false if the operation was aborted.</returns>
        public Future<bool> Add (TangleKey key, T value) {
            return QueueWorkItem(new SetThunk(key, ref value, false));
        }

        /// <summary>
        /// Stores a value into the tangle, assigning it a given key. If the given key already has an associated value, a callback is invoked to determine the new value for the key.
        /// </summary>
        /// <returns>A future that completes once the value has been stored to disk.</returns>
        public Future<bool> AddOrUpdate (TangleKey key, T value, UpdateCallback<T> updateCallback) {
            return QueueWorkItem(new UpdateThunk(key, ref value, updateCallback));
        }

        /// <summary>
        /// Stores a value into the tangle, assigning it a given key. If the given key already has an associated value, a callback is invoked to determine the new value for the key.
        /// </summary>
        /// <returns>A future that completes once the value has been stored to disk.</returns>
        public Future<bool> AddOrUpdate (TangleKey key, T value, DecisionUpdateCallback<T> updateCallback) {
            return QueueWorkItem(new UpdateThunk(key, ref value, updateCallback));
        }

        /// <summary>
        /// Queues a work item into the tangle's work queue. Work items in the queue are processed sequentially in order to prevent corruption of internal data structures.
        /// </summary>
        /// <typeparam name="U">The type of the work item's result, if any.</typeparam>
        /// <param name="workItem">The work item.</param>
        /// <returns>A future that will contain the result of the work item once it is complete.</returns>
        internal Future<U> QueueWorkItem<U> (IWorkItemWithFuture<T, U> workItem) {
            if (_IsDisposed)
                throw new ObjectDisposedException("Tangle");

            var future = workItem.Future;

            if (_WorkerThread == null)
                _WorkerThread = new Squared.Task.Internal.WorkerThread<ConcurrentQueue<IWorkItem<T>>>(
                    WorkerThreadFunc, ThreadPriority.Normal, String.Format("Tangle<{0}> Worker", typeof(T).ToString())
                );

            _WorkerThread.WorkItems.Enqueue(workItem);

            _WorkerThread.Wake();

            return future;
        }

        internal void WorkerThreadFunc (ConcurrentQueue<IWorkItem<T>> workItems, ManualResetEventSlim newWorkItemEvent) {
            while (true) {
                IWorkItem<T> item;
                while (workItems.TryDequeue(out item)) {
                    item.Execute(this);
                }

                if (!newWorkItemEvent.Wait(WorkerThreadTimeoutMs))
                    return;

                newWorkItemEvent.Reset();
            }
        }

        private StreamRange AccessBTreeNode (long index, MemoryMappedFileAccess access) {
            unchecked {
                long position = BTreeHeader.Size + (index * BTreeNode.TotalSize);
                return IndexStream.AccessRange(position, BTreeNode.TotalSize, access);
            }
        }

        private StreamRange AccessBTreeValue (long nodeIndex, uint valueIndex, MemoryMappedFileAccess access) {
            unchecked {
                long position = BTreeHeader.Size + ((nodeIndex * BTreeNode.TotalSize) + BTreeNode.OffsetOfValues + (valueIndex * IndexEntry.Size));
                return IndexStream.AccessRange(position, IndexEntry.Size, access);
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
        private bool SearchValues (IndexEntry * entries, ushort entryCount, byte * pSearchKey, uint searchKeyLength, out uint resultIndex) {
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

                compareLength = Math.Min(Math.Min(pEntry->KeyLength, searchKeyLength), IndexEntry.KeyPrefixSize);

                // Calling out to a function here introduces unnecessary overhead since the prefix is so small
                for (uint i = 0; i < compareLength; i++) {
                    delta = pEntry->KeyPrefix[i] - pSearchKey[i];
                    if (delta != 0)
                        break;
                }

                if ((delta == 0) && (pEntry->KeyLength > IndexEntry.KeyPrefixSize))
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
        private bool FindKey (TangleKey key, out long nodeIndex, out uint valueIndex) {
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
        private bool FindKey (TangleKey key, bool forInsertion, out long nodeIndex, out uint valueIndex, out long parentNodeIndex, out uint parentValueIndex) {
            uint keyLength = (uint)key.Data.Count;

            long nodeCount = BTreeNodeCount;
            long rootIndex = BTreeRootIndex;
            parentNodeIndex = rootIndex;
            parentValueIndex = 0;
            long currentNode = parentNodeIndex;

            fixed (byte * pKey = &key.Data.Array[key.Data.Offset])
            while (currentNode >= 0 && currentNode < nodeCount)
            using (var range = AccessBTreeNode(currentNode, MemoryMappedFileAccess.Read)) {
                var pNode = (BTreeNode *)range.Pointer;
                if (pNode->IsValid == 0)
                    throw new InvalidDataException();

                // As we descend the tree, we split any full nodes we encounter so that
                //  if we end up performing an insertion, we won't need to then walk all the
                //  way back *up* the tree and split in reverse.
                if (forInsertion && (pNode->NumValues == BTreeNode.MaxValues)) {
                    if (parentNodeIndex == currentNode) {
                        // Splitting the root.
                        var newRootIndex = CreateBTreeRoot();

                        using (var newRootRange = AccessBTreeNode(newRootIndex, MemoryMappedFileAccess.ReadWrite)) {
                            var pNewRoot = (BTreeNode *)newRootRange.Pointer;
                            var pNewLeaves = (BTreeLeaf *)(newRootRange.Pointer + BTreeNode.OffsetOfLeaves);
                            
                            // This looks wrong, but it's not: We want the new root to contain 0 values,
                            //  but have one leaf pointing to the old root, so that we can split the old
                            //  root in half. Splitting will move a single value up into the new root.
                            pNewRoot->HasLeaves = 1;
                            pNewLeaves[0].NodeIndex = (uint)currentNode;
                        }

                        BTreeSplitLeafNode(newRootIndex, 0, currentNode);

                        // Restart at the root
                        nodeCount += 2;
                        currentNode = parentNodeIndex = rootIndex = newRootIndex;
                        parentValueIndex = 0;
                        continue;
                    } else {
                        // Splitting a regular node.
                        BTreeSplitLeafNode(parentNodeIndex, parentValueIndex, currentNode);

                        // Restart at the root
                        nodeCount += 1;
                        currentNode = parentNodeIndex = rootIndex;
                        parentValueIndex = 0;
                        continue;
                    }
                }

                var pValues = (IndexEntry*)(range.Pointer + BTreeNode.OffsetOfValues);
                if (SearchValues(pValues, pNode->NumValues, pKey, keyLength, out valueIndex)) {
                    // Found an exact match within the BTree node.
                    nodeIndex = currentNode;
                    return true;
                }

                if (pNode->HasLeaves == 1) {
                    // No exact match was found, so valueIndex now contains the index of the leaf node.
                    var pLeaves = (BTreeLeaf *)(range.Pointer + BTreeNode.OffsetOfLeaves);

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
        private StreamRange BTreePrepareForInsert (long nodeIndex, uint valueIndex) {
            var range = AccessBTreeNode(nodeIndex, MemoryMappedFileAccess.ReadWrite);

            var pNode = (BTreeNode *)range.Pointer;
            var pValues = (IndexEntry *)(range.Pointer + BTreeNode.OffsetOfValues);
            var pLeaves = (BTreeLeaf *)(range.Pointer + BTreeNode.OffsetOfLeaves);

            if (pNode->IsValid == 0)
                throw new InvalidDataException();
            if (pNode->NumValues >= BTreeNode.MaxValues)
                throw new InvalidDataException();

            pNode->IsValid = 0;

            if (valueIndex < pNode->NumValues) {
                var destPtr = (byte*)(&pValues[valueIndex + 1]);
                var copyLength = (byte *)pLeaves - destPtr;
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
        /// <param name="value">The IndexEntry containing information on the value. It must be fully filled in.</param>
        /// <param name="leaf">The leaf associated with the new value.</param>
        private void BTreeInsert (long nodeIndex, uint valueIndex, ref IndexEntry value, ref BTreeLeaf leaf) {
            using (var range = BTreePrepareForInsert(nodeIndex, valueIndex)) {
                var pNode = (BTreeNode *)range.Pointer;
                var pValues = (IndexEntry *)(range.Pointer + BTreeNode.OffsetOfValues);
                var pLeaves = (BTreeLeaf *)(range.Pointer + BTreeNode.OffsetOfLeaves);

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
        private void BTreeSplitLeafNode (long parentNodeIndex, uint leafValueIndex, long leafNodeIndex) {
            const int tMinus1 = BTreeNode.T - 1;

            long newIndex = CreateBTreeNode();

            using (var leafRange = AccessBTreeNode(leafNodeIndex, MemoryMappedFileAccess.ReadWrite))
            using (var newRange = AccessBTreeNode(newIndex, MemoryMappedFileAccess.ReadWrite)) {
                var pLeaf = (BTreeNode *)leafRange.Pointer;
                var pLeafValues = (IndexEntry *)(leafRange.Pointer + BTreeNode.OffsetOfValues);
                var pLeafLeaves = (BTreeLeaf *)(leafRange.Pointer + BTreeNode.OffsetOfLeaves);

                var pNew = (BTreeNode *)newRange.Pointer;
                var pNewValues = (IndexEntry *)(newRange.Pointer + BTreeNode.OffsetOfValues);
                var pNewLeaves = (BTreeLeaf *)(newRange.Pointer + BTreeNode.OffsetOfLeaves);

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

                Native.memmove((byte *)pNewValues, (byte *)&pLeafValues[BTreeNode.T], new UIntPtr(tMinus1 * IndexEntry.Size));

                if (pLeaf->HasLeaves == 1)
                    Native.memmove((byte*)pNewLeaves, (byte*)&pLeafLeaves[BTreeNode.T], new UIntPtr(BTreeNode.T * BTreeLeaf.Size));

                pNew->IsValid = 1;
                pLeaf->IsValid = 1;

                var newLeaf = new BTreeLeaf(newIndex);
                BTreeInsert(
                    parentNodeIndex, leafValueIndex,
                    ref pLeafValues[tMinus1], ref newLeaf
                );
            }
        }

        /// <summary>
        /// Allocates space for a new BTree node.
        /// </summary>
        /// <returns>The index of the new node.</returns>
        private long CreateBTreeNode () {
            long offset = IndexStream.AllocateSpace(BTreeNode.TotalSize).Value;
            var newIndex = (offset - BTreeHeader.Size) / BTreeNode.TotalSize;

            return newIndex;
        }

        private long CreateBTreeRoot () {
            return CreateBTreeRoot(BTreeRootIndex);
        }

        /// <summary>
        /// Creates a new BTree root node, replacing the old root node.
        /// </summary>
        /// <param name="oldRootIndex">The index of the old root node.</param>
        /// <returns>The index of the new root node.</returns>
        private long CreateBTreeRoot (long oldRootIndex) {
            var newIndex = CreateBTreeNode();

            using (var range = AccessBTreeNode(newIndex, MemoryMappedFileAccess.Write)) {
                var pNode = (BTreeNode *)range.Pointer;
                *pNode = new BTreeNode {
                    HasLeaves = 0,
                    IsValid = 1,
                    NumValues = 0
                };
            }

            var pHeader = (BTreeHeader *)_HeaderRange.Pointer;
            if (Interlocked.CompareExchange(ref pHeader->RootIndex, newIndex, oldRootIndex) != oldRootIndex)
                throw new InvalidDataException();

            return newIndex;
        }

        private void InitializeBTree () {
            IndexStream.AllocateSpace(BTreeHeader.Size);

            CreateBTreeRoot();
        }

        private long BTreeRootIndex {
            get {
                var pHeader = (BTreeHeader*)_HeaderRange.Pointer;
                return pHeader->RootIndex;
            }
        }

        private long BTreeNodeCount {
            get {
                return (IndexStream.Length - BTreeHeader.Size) / BTreeNode.TotalSize;
            }
        }

        /// <summary>
        /// The number of values currently stored in the tangle.
        /// </summary>
        public long Count {
            get {
                var pHeader = (BTreeHeader*)_HeaderRange.Pointer;
                return pHeader->ItemCount;
            }
        }

        /// <summary>
        /// The number of bytes wasted within the tangle's data stream by discarded data.
        /// </summary>
        public long WastedDataBytes {
            get {
                var pHeader = (BTreeHeader*)_HeaderRange.Pointer;
                return pHeader->WastedDataBytes;
            }
        }

        private void ReadData (ref IndexEntry entry, out T value) {
            if (entry.KeyType == 0)
                throw new InvalidDataException();

            fixed (IndexEntry * pEntry = &entry)
            using (var range = DataStream.AccessRange(entry.DataOffset, entry.DataLength)) {
                var context = new DeserializationContext(_GetKeyOfEntry, pEntry, range.Pointer, entry.DataLength);
                try {
                    Deserializer(ref context, out value);
                } finally {
                    context.Dispose();
                }
            }
        }

        private static void ReadBytes (byte * ptr, long offset, byte[] buffer, long bufferOffset, uint count) {
            fixed (byte* pDest = &buffer[bufferOffset])
                Native.memmove(pDest, ptr + offset, new UIntPtr((uint)count));
        }

        private static void WriteBytes (byte* ptr, long offset, ArraySegment<byte> bytes) {
            WriteBytes(ptr, offset, bytes.Array, bytes.Offset, bytes.Count);
        }

        private static void WriteBytes (byte* ptr, long offset, byte[] source, int sourceOffset, int count) {
            fixed (byte* pSource = &source[sourceOffset])
                Native.memmove(ptr + offset, pSource, new UIntPtr((uint)count));
        }

        private static void ZeroBytes (byte* ptr, long offset, uint count) {
            Native.memset(ptr + offset, 0, new UIntPtr((uint)count));
        }

        private void InternalSetFoundValue (long nodeIndex, uint valueIndex, ref T value) {
            using (var range = AccessBTreeValue(nodeIndex, valueIndex, MemoryMappedFileAccess.Write)) {
                var pEntry = (IndexEntry *)range.Pointer;

                var keyType = pEntry->KeyType;

                if (keyType == 0)
                    throw new InvalidDataException();

                var segment = Serialize(pEntry, keyType, ref value);
                uint count = (uint)segment.Count;

                long? dataOffset = pEntry->DataOffset;
                WriteModes writeMode = (segment.Count > pEntry->DataLength + pEntry->ExtraDataBytes) ?
                    WriteModes.AppendData : WriteModes.ReplaceData;

                if (writeMode == WriteModes.AppendData)
                    dataOffset = DataStream.AllocateSpace(count);

                pEntry->KeyType = 0;
                WriteData(ref *pEntry, ref segment, writeMode, dataOffset);
                pEntry->KeyType = keyType;
            }
        }

        private void WriteKey (ref IndexEntry indexEntry, TangleKey key) {
            var prefixSize = Math.Min(key.Data.Count, IndexEntry.KeyPrefixSize);

            fixed (byte* pPrefix = indexEntry.KeyPrefix)
                WriteBytes(pPrefix, 0, key.Data.Array, key.Data.Offset, prefixSize);

            if (prefixSize < key.Data.Count) {
                using (var keyRange = KeyStream.AccessRange(indexEntry.KeyOffset, indexEntry.KeyLength, MemoryMappedFileAccess.Write))
                    WriteBytes(keyRange.Pointer, 0, key.Data);
            }
        }

        // IndexEntry must be fully prepared for the write operation:
        //  KeyOffset/KeyLength must be filled in.
        //  DataOffset/DataLength must be filled in.
        //  The IndexEntry's IsValid must be 0.
        // newOffset must specify the offset within the data stream where the data is to be written.
        //  In most cases this should be equal to DataOffset, but in the case of AppendData it will be different.
        private void WriteData (ref IndexEntry indexEntry, ref ArraySegment<byte> data, WriteModes writeMode, long? dataOffset) {
            if (indexEntry.KeyType != 0)
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
                using (var range = DataStream.AccessRange(indexEntry.DataOffset, indexEntry.DataLength, MemoryMappedFileAccess.Write))
                    ZeroBytes(range.Pointer, 0, indexEntry.DataLength);

                Interlocked.Add(ref pHeader->WastedDataBytes, indexEntry.DataLength + indexEntry.ExtraDataBytes);

                indexEntry.DataOffset = (uint)dataOffset.Value;
                indexEntry.DataLength = count;
                indexEntry.ExtraDataBytes = 0;
            } else if (writeMode != WriteModes.ReplaceData) {
                if (dataOffset.HasValue)
                    indexEntry.DataOffset = (uint)dataOffset.Value;

                indexEntry.DataLength = count;
                indexEntry.ExtraDataBytes = 0;
            }

            using (var range = DataStream.AccessRange(indexEntry.DataOffset, indexEntry.DataLength + indexEntry.ExtraDataBytes, MemoryMappedFileAccess.Write)) {
                WriteBytes(range.Pointer, 0, data);

                if (writeMode == WriteModes.ReplaceData) {
                    var bytesToZero = (indexEntry.DataLength + indexEntry.ExtraDataBytes) - count;
                    if (bytesToZero > 0)
                        ZeroBytes(range.Pointer, count, bytesToZero);

                    indexEntry.ExtraDataBytes += bytesToZero;
                    indexEntry.DataLength = count;
                }
            }
        }

        private bool InternalSet (TangleKey key, ref T value, IReplaceCallback<T> replacementCallback) {
            long? dataOffset = null;

            long nodeIndex, parentNodeIndex;
            uint valueIndex, parentValueIndex;

            Exception serializerException = null;
            bool foundExisting = FindKey(key, true, out nodeIndex, out valueIndex, out parentNodeIndex, out parentValueIndex);
            StreamRange range;

            if (!foundExisting) {
                // Prepare BTree for insert. Note that once we have done this, we must successfully insert or
                //  the index will be left in an invalid state!
                range = BTreePrepareForInsert(nodeIndex, valueIndex);
            } else {
                range = AccessBTreeNode(nodeIndex, MemoryMappedFileAccess.ReadWrite);
            }

            using (range) {
                var pEntry = (IndexEntry*)(range.Pointer + BTreeNode.OffsetOfValues + (valueIndex * IndexEntry.Size));

                if (foundExisting) {
                    if (pEntry->KeyType != key.OriginalTypeId)
                        throw new InvalidDataException();

                    bool shouldContinue = replacementCallback.ShouldReplace(this, ref *pEntry, ref value);
                    if (!shouldContinue)
                        return false;

                    pEntry->KeyType = 0;
                } else {
                    pEntry->KeyType = 0;
                    if (key.Data.Count > IndexEntry.KeyPrefixSize)
                        pEntry->KeyOffset = (uint)KeyStream.AllocateSpace((uint)key.Data.Count).Value;
                    else
                        pEntry->KeyOffset = 0;

                    pEntry->KeyLength = (ushort)key.Data.Count;
                    pEntry->DataOffset = 0;
                    pEntry->DataLength = 0;
                    pEntry->ExtraDataBytes = 0;

                    WriteKey(ref *pEntry, key);
                }

                // It is very important that the entry be properly initialized at this point.
                // Serializers can request the key of the value being serialized, in which case the
                //  SerializationContext will attempt to read information from the IndexEntry.
                // Note that since a KeyType of 0 is used to indicate that an entry is being modified,
                //  we pass the actual KeyType to the serializer.

                ArraySegment<byte> segment = default(ArraySegment<byte>);
                try {
                    segment = Serialize(pEntry, key.OriginalTypeId, ref value);
                } catch (Exception ex) {
                    serializerException = ex;
                }

                WriteModes writeMode;
                if (foundExisting) {
                    if (segment.Count > pEntry->DataLength + pEntry->ExtraDataBytes)
                        writeMode = WriteModes.AppendData;
                    else
                        writeMode = WriteModes.ReplaceData;
                } else {
                    writeMode = WriteModes.AppendDataAndKey;
                }

                if (writeMode != WriteModes.ReplaceData)
                    dataOffset = DataStream.AllocateSpace((uint)segment.Count);
                else
                    dataOffset = pEntry->DataOffset;

                if (writeMode == WriteModes.AppendDataAndKey) {
                    pEntry->DataOffset = (uint)dataOffset.GetValueOrDefault(0);
                    pEntry->DataLength = (uint)segment.Count;
                    pEntry->ExtraDataBytes = 0;
                }

                WriteData(ref *pEntry, ref segment, writeMode, dataOffset);

                pEntry->KeyType = key.OriginalTypeId;

                if (!foundExisting) {
                    // Finalize BTree after insert

                    var pNode = (BTreeNode*)range.Pointer;
                    if (pNode->IsValid != 0)
                        throw new InvalidDataException();
                    if (pNode->HasLeaves != 0)
                        throw new InvalidDataException();

                    pNode->IsValid = 1;

                    var pHeader = (BTreeHeader*)_HeaderRange.Pointer;
                    Interlocked.Increment(ref pHeader->ItemCount);
                }
            }

            // If the user's serializer throws, we wait until now to rethrow the exception so that
            //  we do not leave the index in an invalid state (in the case of a BTree insert).
            if (serializerException != null)
                throw new SerializerThrewException(key, serializerException);

            return true;
        }

        private bool ReadKey (IndexEntry * pEntry, ushort keyType, out TangleKey key) {
            if (keyType == 0) {
                key = default(TangleKey);
                return false;
            }

            var buffer = ImmutableArrayPool<byte>.Allocate(pEntry->KeyLength);

            if (pEntry->KeyLength <= IndexEntry.KeyPrefixSize) {
                fixed (byte * pBuffer = buffer.Array)
                    Native.memmove(pBuffer + buffer.Offset, pEntry->KeyPrefix, new UIntPtr(pEntry->KeyLength));
            } else {
                using (var keyRange = KeyStream.AccessRange(
                    pEntry->KeyOffset, pEntry->KeyLength, MemoryMappedFileAccess.Read
                ))
                    ReadBytes(keyRange.Pointer, 0, buffer.Array, buffer.Offset, pEntry->KeyLength);
            }

            key = new TangleKey(buffer, keyType);
            return true;
        }

        private bool InternalFind (TangleKey key, out FindResult result) {
            long nodeIndex;
            uint valueIndex;

            if (!FindKey(key, out nodeIndex, out valueIndex)) {
                result = default(FindResult);
                return false;
            }

            result = new FindResult(this, key, nodeIndex, valueIndex);
            return true;
        }

        private bool InternalGet (TangleKey key, out T value) {
            long nodeIndex;
            uint valueIndex;

            if (!FindKey(key, out nodeIndex, out valueIndex)) {
                value = default(T);
                return false;
            }

            using (var range = AccessBTreeValue(nodeIndex, valueIndex, MemoryMappedFileAccess.Read)) {
                var pEntry = (IndexEntry *)range.Pointer;
                ReadData(ref *pEntry, out value);
            }
            
            return true;
        }

        private int InternalGetNodeValues (long nodeIndex, T[] output, int outputOffset) {
            using (var range = AccessBTreeNode(nodeIndex, MemoryMappedFileAccess.Read)) {
                var pNode = (BTreeNode *)range.Pointer;

                if (pNode->IsValid != 1)
                    throw new InvalidDataException();

                var numValues = pNode->NumValues;
                for (int i = 0; i < numValues; i++) {
                    var pEntry = (IndexEntry *)(range.Pointer + BTreeNode.OffsetOfValues + (i * IndexEntry.Size));

                    ReadData(ref *pEntry, out output[i + outputOffset]);
                }

                return numValues;
            }
        }

        private int InternalGetNodeKeys (long nodeIndex, TangleKey[] output, int outputOffset) {
            using (var range = AccessBTreeNode(nodeIndex, MemoryMappedFileAccess.Read)) {
                var pNode = (BTreeNode*)range.Pointer;

                if (pNode->IsValid != 1)
                    throw new InvalidDataException();

                var numValues = pNode->NumValues;
                for (int i = 0; i < numValues; i++) {
                    var pEntry = (IndexEntry*)(range.Pointer + BTreeNode.OffsetOfValues + (i * IndexEntry.Size));

                    ReadKey(pEntry, pEntry->KeyType, out output[i + outputOffset]);
                }

                return numValues;
            }
        }

        private void InternalGetFoundValue (long nodeIndex, uint valueIndex, out T result) {
            using (var range = AccessBTreeValue(nodeIndex, valueIndex, MemoryMappedFileAccess.Read)) {
                var pEntry = (IndexEntry *)range.Pointer;
                ReadData(ref *pEntry, out result);
            }
        }

        public void Dispose () {
            if (_IsDisposed)
                return;
            _IsDisposed = true;

            if (_WorkerThread != null) {
                var workItems = _WorkerThread.WorkItems;
                _WorkerThread.Dispose();
                _WorkerThread = null;

                IWorkItem<T> wi;
                while (workItems.TryDequeue(out wi))
                    wi.Dispose();
            }

            IndexStream.Dispose();
            DataStream.Dispose();
            KeyStream.Dispose();

            if (OwnsStorage)
                Storage.Dispose();
        }
    }
}