/*
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

        public static readonly int WorkerThreadTimeoutMs = 30000;

        public readonly bool OwnsStorage;
        public readonly StreamSource Storage;
        public readonly TaskScheduler Scheduler;
        public readonly Serializer<T> Serializer;
        public readonly Deserializer<T> Deserializer;
        public readonly Dictionary<string, IndexBase<T>> Indices = new Dictionary<string, IndexBase<T>>();

        protected readonly ReaderWriterLockSlim IndexLock = new ReaderWriterLockSlim(LockRecursionPolicy.NoRecursion);

        internal Squared.Task.Internal.WorkerThread<ConcurrentQueue<IWorkItem<T>>> _WorkerThread;

        private bool _IsDisposed;

        private readonly BTree BTree;

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

            BTree = new BTree(Storage, "");
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
        public Batch<T> CreateBatch (int capacity) {
            return new Batch<T>(this, capacity);
        }

        public Future<Index<U, T>> CreateIndex<U> (string name, IndexFunc<U, T> function) {
            return Index<U, T>.Create(this, name, function);
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
            Tangle<TRight>.JoinBarrierThunk rightBarrier = null;
            if (!Object.Equals(right, this)) {
                rightBarrier = new Tangle<TRight>.JoinBarrierThunk();
                right.QueueWorkItem(rightBarrier);
            }

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

        internal long NodeCount {
            get {
                return BTree.NodeCount;
            }
        }

        public long Count {
            get {
                return BTree.ValueCount;
            }
        }

        public long WastedDataBytes {
            get {
                return BTree.WastedDataBytes;
            }
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

        private void InternalSetFoundValue (long nodeIndex, uint valueIndex, ref T value) {
            using (var range = BTree.AccessNode(nodeIndex, true)) {
                ushort keyType;
                var pEntry = BTree.LockValue(range, valueIndex, out keyType);

                if (Indices.Count > 0) {
                    TangleKey key;
                    T oldValue;

                    BTree.ReadKey(pEntry, keyType, out key);
                    ReadData(ref *pEntry, keyType, out oldValue);

                    foreach (var index in Indices.Values) {
                        index.OnValueRemoved(key, ref oldValue);
                        index.OnValueAdded(key, ref value);
                    }
                }

                var segment = BTree.Serialize(pEntry, Serializer, keyType, ref value);

                BTree.WriteData(pEntry, segment);

                BTree.UnlockValue(pEntry, keyType);

                BTree.UnlockNode(range);
            }
        }

        private bool InternalSet (TangleKey key, ref T value, IReplaceCallback<T> replacementCallback) {
            long nodeIndex;
            uint valueIndex;

            Exception serializerException = null;
            bool foundExisting = BTree.FindKey(key, true, out nodeIndex, out valueIndex);

            StreamRange range;
            if (foundExisting) {
                range = BTree.AccessNode(nodeIndex, true);
            } else {
                // Prepare BTree for insert. Note that once we have done this, we must successfully insert or
                //  the index will be left in an invalid state!
                range = BTree.PrepareForInsert(nodeIndex, valueIndex);
            }

            using (range) {
                var pEntry = BTree.LockValue(range, valueIndex, foundExisting ? key.OriginalTypeId : (ushort)0);

                if (foundExisting) {
                    bool shouldContinue = replacementCallback.ShouldReplace(this, ref *pEntry, key.OriginalTypeId, ref value);

                    if (!shouldContinue) {
                        BTree.UnlockValue(pEntry, key.OriginalTypeId);
                        BTree.UnlockNode(range);
                        return false;
                    }
                } else {
                    BTree.WriteNewKey(pEntry, key);
                }

                // It is very important that the entry be properly initialized at this point.
                // Serializers can request the key of the value being serialized, in which case the
                //  SerializationContext will attempt to read information from the IndexEntry.
                // Note that since a KeyType of 0 is used to indicate that an entry is being modified,
                //  we pass the actual KeyType to the serializer.

                ArraySegment<byte> segment = default(ArraySegment<byte>);
                try {
                    segment = BTree.Serialize(pEntry, Serializer, key.OriginalTypeId, ref value);
                } catch (Exception ex) {
                    serializerException = ex;
                }

                if ((Indices.Count > 0) && foundExisting) {
                    T oldValue;
                    ReadData(ref *pEntry, key.OriginalTypeId, out oldValue);

                    foreach (var index in Indices.Values)
                        index.OnValueRemoved(key, ref oldValue);
                }

                BTree.WriteData(pEntry, segment);

                BTree.UnlockValue(pEntry, key.OriginalTypeId);

                if (foundExisting)
                    BTree.UnlockNode(range);
                else
                    BTree.FinalizeInsert(range);
            }

            foreach (var index in Indices.Values)
                index.OnValueAdded(key, ref value);

            // If the user's serializer throws, we wait until now to rethrow the exception so that
            //  we do not leave the index in an invalid state (in the case of a BTree insert).
            if (serializerException != null)
                throw new SerializerThrewException(key, serializerException);

            return true;
        }

        private bool InternalFind (TangleKey key, out FindResult result) {
            long nodeIndex;
            uint valueIndex;

            if (!BTree.FindKey(key, false, out nodeIndex, out valueIndex)) {
                result = default(FindResult);
                return false;
            }

            result = new FindResult(this, key, nodeIndex, valueIndex);
            return true;
        }

        internal bool InternalGet (TangleKey key, out T value) {
            long nodeIndex;
            uint valueIndex;

            if (!BTree.FindKey(key, false, out nodeIndex, out valueIndex)) {
                value = default(T);
                return false;
            }

            using (var range = BTree.AccessValue(nodeIndex, valueIndex)) {
                var pEntry = (BTreeValue *)range.Pointer;
                BTree.ReadData(ref *pEntry, Deserializer, out value);
            }
            
            return true;
        }

        private unsafe ushort GetValueCount (StreamRange range) {
            var pNode = (BTreeNode*)range.Pointer;

            return pNode->NumValues;
        }

        private unsafe KeyValuePair<TangleKey, T> GetNodeKeyValuePair (StreamRange range, uint valueIndex) {
            var pEntry = (BTreeValue*)(range.Pointer + BTreeNode.OffsetOfValues + (valueIndex * BTreeValue.Size));

            TangleKey key;
            T value;
            BTree.ReadKey(pEntry, out key);
            BTree.ReadData(pEntry, Deserializer, out value);

            return new KeyValuePair<TangleKey, T>(key, value);
        }

        internal IEnumerable<KeyValuePair<TangleKey, T>> InternalEnumerateNode (long nodeIndex) {
            using (var range = BTree.AccessNode(nodeIndex, false)) {
                var numValues = GetValueCount(range);

                for (uint i = 0; i < numValues; i++)
                    yield return GetNodeKeyValuePair(range, i);
            }
        }

        internal int InternalGetNodeValues (long nodeIndex, T[] output, int outputOffset) {
            using (var range = BTree.AccessNode(nodeIndex, false)) {
                var pNode = (BTreeNode *)range.Pointer;

                var numValues = pNode->NumValues;
                for (int i = 0; i < numValues; i++) {
                    var pEntry = (BTreeValue *)(range.Pointer + BTreeNode.OffsetOfValues + (i * BTreeValue.Size));

                    BTree.ReadData(pEntry, Deserializer, out output[i + outputOffset]);
                }

                return numValues;
            }
        }

        internal int InternalGetNodeKeys (long nodeIndex, TangleKey[] output, int outputOffset) {
            using (var range = BTree.AccessNode(nodeIndex, false)) {
                var pNode = (BTreeNode*)range.Pointer;

                var numValues = pNode->NumValues;
                for (int i = 0; i < numValues; i++) {
                    var pEntry = (BTreeValue*)(range.Pointer + BTreeNode.OffsetOfValues + (i * BTreeValue.Size));

                    BTree.ReadKey(pEntry, out output[i + outputOffset]);
                }

                return numValues;
            }
        }

        private void InternalGetFoundValue (long nodeIndex, uint valueIndex, out T result) {
            using (var range = BTree.AccessValue(nodeIndex, valueIndex)) {
                var pEntry = (BTreeValue *)range.Pointer;

                BTree.ReadData(ref *pEntry, Deserializer, out result);
            }
        }

        private unsafe void ReadData (ref BTreeValue entry, ushort keyType, out T value) {
            fixed (BTreeValue * pEntry = &entry)
                BTree.ReadData(pEntry, keyType, Deserializer, out value);
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

            BTree.Dispose();

            foreach (var index in Indices.Values)
                index.Dispose();
            Indices.Clear();

            if (OwnsStorage)
                Storage.Dispose();
        }
    }
}