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
using Squared.Data.Mangler.Serialization;
using Squared.Task;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading;
using Squared.Data.Mangler.Internal;
using System.IO.MemoryMappedFiles;
using System.Collections.Concurrent;

namespace Squared.Data.Mangler.Internal {
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    internal unsafe struct BTreeNode {
        public static readonly uint Size;
        public static readonly uint TotalSize;
        public static readonly uint OffsetOfValues;
        public static readonly uint OffsetOfLeaves;

        public const int T = 32;
        public const int MaxValues = (2 * T) - 1;
        public const int MaxLeaves = MaxValues + 1;

        static BTreeNode () {
            if (T % 2 != 0)
                throw new InvalidDataException();

            Size = (uint)Marshal.SizeOf(typeof(BTreeNode));
            TotalSize = Size + (MaxValues * IndexEntry.Size) + (MaxLeaves * BTreeLeaf.Size);
            OffsetOfValues = Size;
            OffsetOfLeaves = OffsetOfValues + (MaxValues * IndexEntry.Size);
        }

        public byte IsValid;
        public byte HasLeaves;
        public ushort NumValues;
    }

    [StructLayout(LayoutKind.Sequential, Pack = 1, Size = 4)]
    internal unsafe struct BTreeLeaf {
        public static readonly uint Size;

        static BTreeLeaf () {
            Size = (uint)Marshal.SizeOf(typeof(BTreeLeaf));
        }

        public uint NodeIndex;

        public BTreeLeaf (long nodeIndex) {
            NodeIndex = (uint)nodeIndex;
        }
    }

    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    internal unsafe struct IndexEntry {
        public const int KeyPrefixSize = 4;
        public static readonly uint Size;

        static IndexEntry () {
            Size = (uint)Marshal.SizeOf(typeof(IndexEntry));
        }

        public uint DataOffset;
        public uint DataLength;
        public uint KeyOffset;
        public ushort KeyLength;
        public ushort KeyType;
        public fixed byte KeyPrefix[KeyPrefixSize];
    }
}

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

    public struct TangleKey {
        private static readonly Dictionary<ushort, Type> TypeIdToType = new Dictionary<ushort, Type>();
        private static readonly Dictionary<Type, ushort> TypeToTypeId = new Dictionary<Type, ushort>();

        static TangleKey () {
            RegisterType<string>();
            RegisterType<byte[]>();
            RegisterType<uint>();
            RegisterType<int>();
            RegisterType<ulong>();
            RegisterType<long>();
        }

        private static void RegisterType<T> () {
            if (TypeToTypeId.Count >= (ushort.MaxValue - 2))
                throw new InvalidOperationException("Too many registered types");

            var type = typeof(T);
            ushort id = (byte)(TypeToTypeId.Count + 1);
            TypeToTypeId[type] = id;
            TypeIdToType[id] = type;
        }

        public readonly ushort OriginalTypeId;
        public readonly ArraySegment<byte> Data;

        public TangleKey (uint key)
            : this(BitConverter.GetBytes(key), TypeToTypeId[typeof(uint)]) {
        }

        public TangleKey (ulong key)
            : this(BitConverter.GetBytes(key), TypeToTypeId[typeof(ulong)]) {
        }

        public TangleKey (int key)
            : this(BitConverter.GetBytes(key), TypeToTypeId[typeof(int)]) {
        }

        public TangleKey (long key)
            : this(BitConverter.GetBytes(key), TypeToTypeId[typeof(long)]) {
        }

        public TangleKey (string key)
            : this (Encoding.ASCII.GetBytes(key), TypeToTypeId[typeof(string)]) {
        }

        public TangleKey (byte[] array)
            : this(array, 0, array.Length, TypeToTypeId[typeof(byte[])]) {
        }

        public TangleKey (byte[] array, int offset, int count)
            : this(array, offset, count, TypeToTypeId[typeof(string)]) {
        }

        public TangleKey (byte[] array, ushort originalType)
            : this(array, 0, array.Length, originalType) {
        }

        public TangleKey (byte[] array, int offset, int count, ushort originalType) {
            if (count >= ushort.MaxValue)
                throw new InvalidDataException("Key too long");
            if (originalType == 0)
                throw new InvalidDataException("Invalid key type");

            Data = new ArraySegment<byte>(array, offset, count);
            OriginalTypeId = originalType;
        }

        public Type OriginalType {
            get {
                return TypeIdToType[OriginalTypeId];
            }
        }

        public object Value {
            get {
                var type = OriginalType;

                if (type == typeof(string)) {
                    return Encoding.ASCII.GetString(Data.Array, Data.Offset, Data.Count);
                } else if (type == typeof(int)) {
                    return BitConverter.ToInt32(Data.Array, Data.Offset);
                } else if (type == typeof(uint)) {
                    return BitConverter.ToUInt32(Data.Array, Data.Offset);
                } else if (type == typeof(long)) {
                    return BitConverter.ToInt64(Data.Array, Data.Offset);
                } else if (type == typeof(ulong)) {
                    return BitConverter.ToUInt64(Data.Array, Data.Offset);
                } else /* if (type == typeof(byte[])) */ {
                    return Data;
                }
            }
        }

        public static implicit operator TangleKey (string key) {
            return new TangleKey(key);
        }

        public static implicit operator TangleKey (uint key) {
            return new TangleKey(key);
        }

        public static implicit operator TangleKey (int key) {
            return new TangleKey(key);
        }

        public static implicit operator TangleKey (ulong key) {
            return new TangleKey(key);
        }

        public static implicit operator TangleKey (long key) {
            return new TangleKey(key);
        }

        public override string ToString () {
            var value = Value;

            if (value is ArraySegment<byte>) {
                var sb = new StringBuilder();
                for (int i = 0; i < Data.Count; i++)
                    sb.AppendFormat("{0:X2}", Data.Array[i + Data.Offset]);
                return sb.ToString();
            } else {
                return value.ToString();
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
    public unsafe class Tangle<T> : IDisposable {
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

            internal FindResult (Tangle<T> owner, ref TangleKey key, long nodeIndex, uint valueIndex) {
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

        public struct SetBatchItem {
            public TangleKey Key;
            public T Value;

            public SetBatchItem (TangleKey key, T value) {
                Key = key;
                Value = value;
            }

            public SetBatchItem (TangleKey key, ref T value) {
                Key = key;
                Value = value;
            }
        }

        public class SetBatch {
            public readonly int Capacity;
            internal readonly SetBatchItem[] Buffer;
            private int _Count;

            public SetBatch (int capacity) {
                Capacity = capacity;
                Buffer = new SetBatchItem[capacity];
            }

            public int Count {
                get {
                    return _Count;
                }
            }

            new public void Add (TangleKey key, T value) {
                if (_Count >= Capacity)
                    throw new IndexOutOfRangeException();

                Buffer[_Count++] = new SetBatchItem(key, value);
            }

            new public void Add (TangleKey key, ref T value) {
                if (_Count >= Capacity)
                    throw new IndexOutOfRangeException();

                Buffer[_Count++] = new SetBatchItem(key, ref value);
            }

            public Future<int> Execute (Tangle<T> tangle, bool replaceExistingItems = true) {
                return tangle.QueueWorkItem(new SetBatchThunk(this, replaceExistingItems));
            }
        }


        /// <summary>
        /// Called to update a value within the tangle.
        /// </summary>
        /// <param name="oldValue">The current value of the item.</param>
        /// <returns>The new value of the item.</returns>
        public delegate T UpdateCallback (T oldValue);

        /// <summary>
        /// Called to update a value within the tangle.
        /// </summary>
        /// <param name="value">The current value of the item. Change it and return true if you wish to modify the item.</param>
        /// <returns>True to update the item's value, false to abort.</returns>
        public delegate bool DecisionUpdateCallback (ref T value);

        private interface IReplaceCallback {
            bool ShouldReplace (Tangle<T> tangle, ref IndexEntry indexEntry, ref T newValue);
        }

        private abstract class ThunkBase<U> : IWorkItemWithFuture<T, U>, IDisposable {
            protected Future<U> Future = new Future<U>();
            protected Exception Failure = null;

            protected abstract void OnExecute (Tangle<T> tangle, out U result);

            protected void Fail (Exception ex) {
                Failure = ex;
            }

            public void Execute (Tangle<T> tangle) {
                try {
                    U result;
                    OnExecute(tangle, out result);

                    if (Failure != null)
                        Future.Fail(Failure);
                    else
                        Future.Complete(result);
                } catch (Exception ex) {
                    Future.Fail(ex);
                }
                Dispose();
            }

            Future<U> IWorkItemWithFuture<T, U>.Future {
                get {
                    return Future;
                }
            }

            public virtual void Dispose () {
                Future = null;
                Failure = null;
            }
        }

        private abstract class SetThunkBase<U> : ThunkBase<U> {
            public T Value;

            public override void Dispose () {
                base.Dispose();
                Value = default(T);
            }
        }

        private class SetThunk : SetThunkBase<bool>, IReplaceCallback {
            public readonly TangleKey Key;
            public readonly bool ShouldReplace;

            public SetThunk (ref TangleKey key, ref T value, bool shouldReplace) {
                Key = key;
                Value = value;
                ShouldReplace = shouldReplace;
            }

            bool IReplaceCallback.ShouldReplace (Tangle<T> tangle, ref IndexEntry indexEntry, ref T newValue) {
                return ShouldReplace;
            }

            protected override void OnExecute (Tangle<T> tangle, out bool result) {
                result = tangle.InternalSet(Key, ref Value, this);
            }
        }

        private class SetBatchThunk : SetThunkBase<int>, IReplaceCallback {
            public SetBatch Batch;
            public readonly bool ShouldReplace;

            public SetBatchThunk (SetBatch batch, bool shouldReplace) {
                Batch = batch;
                ShouldReplace = shouldReplace;
            }

            bool IReplaceCallback.ShouldReplace (Tangle<T> tangle, ref IndexEntry indexEntry, ref T newValue) {
                return ShouldReplace;
            }

            protected override void OnExecute (Tangle<T> tangle, out int result) {
                result = 0;

                var items = Batch.Buffer;
                for (int i = 0, c = Batch.Count; i < c; i++) {
                    if (tangle.InternalSet(items[i].Key, ref items[i].Value, this))
                        result += 1;
                }
            }

            public override void Dispose () {
                base.Dispose();
                Batch = null;
            }
        }

        private class UpdateThunk : SetThunkBase<bool>, IReplaceCallback {
            public readonly TangleKey Key;
            public readonly UpdateCallback Callback;
            public readonly DecisionUpdateCallback DecisionCallback;

            public UpdateThunk (ref TangleKey key, ref T value, UpdateCallback callback) {
                Key = key;
                Value = value;
                Callback = callback;
                DecisionCallback = null;
            }

            public UpdateThunk (ref TangleKey key, ref T value, DecisionUpdateCallback callback) {
                Key = key;
                Value = value;
                Callback = null;
                DecisionCallback = callback;
            }

            bool IReplaceCallback.ShouldReplace (Tangle<T> tangle, ref IndexEntry indexEntry, ref T newValue) {
                if (Callback != null) {
                    T oldValue;
                    tangle.ReadData(ref indexEntry, out oldValue);
                    newValue = Callback(oldValue);
                    return true;
                } else {
                    tangle.ReadData(ref indexEntry, out newValue);
                    return DecisionCallback(ref newValue);
                }
            }

            protected override void OnExecute(Tangle<T> tangle, out bool result) {
                result = tangle.InternalSet(Key, ref Value, this);
            }
        }

        private class GetThunk : ThunkBase<T> {
            public readonly TangleKey Key;

            public GetThunk (ref TangleKey key) {
                Key = key;
            }

            protected override void OnExecute (Tangle<T> tangle, out T result) {
                if (!tangle.InternalGet(Key, out result))
                    Fail(new KeyNotFoundException(Key));
            }
        }

        private class FindThunk : ThunkBase<FindResult> {
            public readonly TangleKey Key;

            public FindThunk (ref TangleKey key) {
                Key = key;
            }

            protected override void OnExecute (Tangle<T> tangle, out FindResult result) {
                if (!tangle.InternalFind(Key, out result))
                    Fail(new KeyNotFoundException(Key));
            }
        }

        private class GetByIndexThunk : ThunkBase<T> {
            public readonly long NodeIndex;
            public readonly uint ValueIndex;

            public GetByIndexThunk (long nodeIndex, uint valueIndex) {
                NodeIndex = nodeIndex;
                ValueIndex = valueIndex;
            }

            protected override void OnExecute (Tangle<T> tangle, out T result) {
                tangle.InternalGetFoundValue(NodeIndex, ValueIndex, out result);
            }
        }

        private class SetByIndexThunk : SetThunkBase<NoneType> {
            public readonly long NodeIndex;
            public readonly uint ValueIndex;

            public SetByIndexThunk (long nodeIndex, uint valueIndex, ref T value) {
                NodeIndex = nodeIndex;
                ValueIndex = valueIndex;
                Value = value;
            }

            protected override void OnExecute (Tangle<T> tangle, out NoneType result) {
                tangle.InternalSetFoundValue(NodeIndex, ValueIndex, ref Value);
                result = NoneType.None;
            }
        }


        public const bool TraceKeyInsertions = false;
        public const uint CurrentFormatVersion = 2;
        public const int MaxSerializationBufferSize = 1024 * 64;

        public static readonly int WorkerThreadTimeoutMs = 30000;

        public readonly bool OwnsStorage;
        public readonly StreamSource Storage;
        public readonly TaskScheduler Scheduler;
        public readonly Serializer<T> Serializer;
        public readonly Deserializer<T> Deserializer;

        protected readonly ReaderWriterLockSlim IndexLock = new ReaderWriterLockSlim(LockRecursionPolicy.NoRecursion);

        internal Squared.Task.Internal.WorkerThread<ConcurrentQueue<IWorkItem<T>>> _WorkerThread;

        private MemoryStream SerializationBuffer;
        private long _WastedDataBytes = 0;

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

            IndexStream = Storage.Open("index");
            KeyStream = Storage.Open("keys");
            DataStream = Storage.Open("data");

            VersionCheck(IndexStream);
            VersionCheck(KeyStream);
            VersionCheck(DataStream);

            if (BTreeNodeCount == 0)
                CreateBTreeRoot();
        }

        private ArraySegment<byte> Serialize (ref T value) {
            if (SerializationBuffer == null)
                SerializationBuffer = new MemoryStream();

            Serializer(ref value, SerializationBuffer);

            var result = new ArraySegment<byte>(SerializationBuffer.GetBuffer(), 0, (int)SerializationBuffer.Length);

            if (SerializationBuffer.Capacity > MaxSerializationBufferSize) {
                SerializationBuffer.Dispose();
                SerializationBuffer = null;
            } else {
                SerializationBuffer.Seek(0, SeekOrigin.Begin);
                SerializationBuffer.SetLength(0);
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

        private static long StreamAppend<U> (StreamRef stream, ref U value) 
            where U : struct {
            uint entrySize = (uint)Marshal.SizeOf(typeof(U));
            long spot = stream.AllocateSpace(entrySize);

            using (var range = stream.AccessRange(spot, entrySize, MemoryMappedFileAccess.Write))
                Unsafe<U>.StructureToPtr(ref value, range.Pointer, entrySize);

            return spot;
        }

        private static long StreamAppend (StreamRef stream, ArraySegment<byte> data) {
            long spot = stream.AllocateSpace((uint)data.Count);

            using (var range = stream.AccessRange(spot, (uint)data.Count, MemoryMappedFileAccess.Write))
                WriteBytes(range.Pointer, 0, data);

            return spot;
        }

        /// <summary>
        /// Reads a value from the tangle, looking it up via its key.
        /// </summary>
        /// <returns>A future that will contain the value once it has been read.</returns>
        /// <exception cref="KeyNotFoundException">If the specified key is not found, the future will contain a KeyNotFoundException.</exception>
        public Future<T> Get (TangleKey key) {
            return QueueWorkItem(new GetThunk(ref key));
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
            return QueueWorkItem(new FindThunk(ref key));
        }

        /// <summary>
        /// Stores a value into the tangle, assigning it a given key. If the given key already has an associated value, that value is replaced.
        /// </summary>
        /// <returns>A future that completes once the value has been stored to disk.</returns>
        public IFuture Set (TangleKey key, T value) {
            return QueueWorkItem(new SetThunk(ref key, ref value, true));
        }

        /// <summary>
        /// Stores a value into the tangle, assigning it a given key. If the given key already has an associated value, the operation will abort.
        /// </summary>
        /// <returns>A future that completes once the value has been stored to disk. The future's value will be false if the operation was aborted.</returns>
        public Future<bool> Add (TangleKey key, T value) {
            return QueueWorkItem(new SetThunk(ref key, ref value, false));
        }

        /// <summary>
        /// Stores a value into the tangle, assigning it a given key. If the given key already has an associated value, a callback is invoked to determine the new value for the key.
        /// </summary>
        /// <returns>A future that completes once the value has been stored to disk.</returns>
        public Future<bool> AddOrUpdate (TangleKey key, T value, UpdateCallback updateCallback) {
            return QueueWorkItem(new UpdateThunk(ref key, ref value, updateCallback));
        }

        /// <summary>
        /// Stores a value into the tangle, assigning it a given key. If the given key already has an associated value, a callback is invoked to determine the new value for the key.
        /// </summary>
        /// <returns>A future that completes once the value has been stored to disk.</returns>
        public Future<bool> AddOrUpdate (TangleKey key, T value, DecisionUpdateCallback updateCallback) {
            return QueueWorkItem(new UpdateThunk(ref key, ref value, updateCallback));
        }

        private Future<U> QueueWorkItem<U> (IWorkItemWithFuture<T, U> workItem) {
            var future = workItem.Future;

            if (_WorkerThread == null)
                _WorkerThread = new Squared.Task.Internal.WorkerThread<ConcurrentQueue<IWorkItem<T>>>(
                    WorkerThreadFunc, ThreadPriority.Normal, String.Format("Tangle<{0}> Worker", typeof(T).ToString())
                );

            _WorkerThread.WorkItems.Enqueue(workItem);

            _WorkerThread.Wake();

            return future;
        }

        internal void WorkerThreadFunc (ConcurrentQueue<IWorkItem<T>> workItems, ManualResetEvent newWorkItemEvent) {
            while (true) {
                IWorkItem<T> item;
                while (workItems.TryDequeue(out item)) {
                    item.Execute(this);
                }

                if (!newWorkItemEvent.WaitOne(WorkerThreadTimeoutMs))
                    return;

                newWorkItemEvent.Reset();
            }
        }

        private StreamRange AccessBTreeNode (long index, MemoryMappedFileAccess access) {
            long count = BTreeNodeCount;

            if ((index < 0) || (index >= count))
                throw new ArgumentException(String.Format(
                    "Expected 0 <= index < {0}, but index was {1}", count, index
                ), "index");

            long position = index * BTreeNode.TotalSize;
            return IndexStream.AccessRange(position, BTreeNode.TotalSize, access);
        }

        private StreamRange AccessBTreeValue (long nodeIndex, uint valueIndex, MemoryMappedFileAccess access) {
            long count = BTreeNodeCount;

            if ((nodeIndex < 0) || (nodeIndex >= count))
                throw new ArgumentException(String.Format(
                    "Expected 0 <= index < {0}, but index was {1}", count, nodeIndex
                ), "index");

            long position = (nodeIndex * BTreeNode.TotalSize) + BTreeNode.OffsetOfValues + (valueIndex * IndexEntry.Size);
            return IndexStream.AccessRange(position, IndexEntry.Size, access);
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
        private unsafe bool SearchValues (IndexEntry * entries, ushort entryCount, byte * pSearchKey, uint searchKeyLength, out uint resultIndex) {
            int min = 0, max = entryCount - 1;
            int delta = 0, pivot;
            uint compareLength;

            while (min <= max) {
                pivot = min + ((max - min) >> 1);

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
        private unsafe bool FindKey (ref TangleKey key, out long nodeIndex, out uint valueIndex) {
            long temp;
            uint temp2;

            return FindKey(ref key, false, out nodeIndex, out valueIndex, out temp, out temp2);
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
        private unsafe bool FindKey (ref TangleKey key, bool forInsertion, out long nodeIndex, out uint valueIndex, out long parentNodeIndex, out uint parentValueIndex) {
            uint keyLength = (uint)key.Data.Count;

            long nodeCount = BTreeNodeCount;
            parentNodeIndex = IndexStream.RootIndex;
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
                            if (pNewRoot->NumValues != 0)
                                throw new InvalidDataException();
                        }

                        BTreeSplitLeafNode(newRootIndex, 0, currentNode);

                        // Restart at the root
                        nodeCount = BTreeNodeCount;
                        currentNode = parentNodeIndex =IndexStream.RootIndex;
                        parentValueIndex = 0;
                        continue;
                    } else {
                        // Splitting a regular node.
                        BTreeSplitLeafNode(parentNodeIndex, parentValueIndex, currentNode);

                        // Restart at the root
                        nodeCount = BTreeNodeCount;
                        currentNode = parentNodeIndex = IndexStream.RootIndex;
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

        private unsafe void BTreePrepareForInsert (long nodeIndex, uint valueIndex) {
            using (var range = AccessBTreeNode(nodeIndex, MemoryMappedFileAccess.ReadWrite)) {
                var pNode = (BTreeNode *)range.Pointer;
                var pValues = (IndexEntry *)(range.Pointer + BTreeNode.OffsetOfValues);
                var pLeaves = (BTreeLeaf *)(range.Pointer + BTreeNode.OffsetOfLeaves);

                if (pNode->IsValid == 0)
                    throw new InvalidDataException();
                if (pNode->NumValues >= BTreeNode.MaxValues)
                    throw new InvalidDataException();

                pNode->IsValid = 0;

                if (valueIndex < pNode->NumValues) {
                    // Move values
                    Native.memmove(
                        (byte*)(&pValues[valueIndex + 1]),
                        (byte*)(&pValues[valueIndex]),
                        new UIntPtr((pNode->NumValues - valueIndex) * IndexEntry.Size)
                    );
                }

                if (pNode->HasLeaves == 1) {
                    // Move leaves
                    Native.memmove(
                        (byte*)(&pLeaves[valueIndex + 2]),
                        (byte*)(&pLeaves[valueIndex + 1]),
                        new UIntPtr((pNode->NumValues - valueIndex) * BTreeLeaf.Size)
                    );
                }

                pNode->NumValues += 1;
            }
        }

        private unsafe void BTreeInsert (long nodeIndex, uint valueIndex, ref IndexEntry value, ref BTreeLeaf leaf) {
            BTreePrepareForInsert(nodeIndex, valueIndex);

            using (var range = AccessBTreeNode(nodeIndex, MemoryMappedFileAccess.ReadWrite)) {
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

        private unsafe void BTreeSplitLeafNode (long parentNodeIndex, uint leafValueIndex, long leafNodeIndex) {
            long newIndex = CreateBTreeNode();
            var tMinus1 = BTreeNode.T - 1;

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

                Native.memmove((byte *)pNewValues, (byte *)&pLeafValues[BTreeNode.T], new UIntPtr((BTreeNode.T - 1) * IndexEntry.Size));

                if (pLeaf->HasLeaves == 1)
                    Native.memmove((byte*)pNewLeaves, (byte*)&pLeafLeaves[BTreeNode.T], new UIntPtr(BTreeNode.T * BTreeLeaf.Size));

                pNew->IsValid = 1;
                pLeaf->IsValid = 1;

                var newLeaf = new BTreeLeaf(newIndex);
                BTreeInsert(
                    parentNodeIndex, leafValueIndex,
                    ref pLeafValues[BTreeNode.T - 1], ref newLeaf
                );
            }
        }

        private unsafe long CreateBTreeNode () {
            long offset = IndexStream.AllocateSpace(BTreeNode.TotalSize);
            var newIndex = offset / BTreeNode.TotalSize;

            return newIndex;
        }

        private unsafe long CreateBTreeRoot () {
            var oldIndex = IndexStream.RootIndex;
            var newIndex = CreateBTreeNode();

            using (var range = AccessBTreeNode(newIndex, MemoryMappedFileAccess.Write)) {
                var pNode = (BTreeNode *)range.Pointer;
                *pNode = new BTreeNode {
                    HasLeaves = 0,
                    IsValid = 1,
                    NumValues = 0
                };
            }

            if (!IndexStream.MoveRoot(oldIndex, newIndex))
                throw new InvalidDataException();

            return newIndex;
        }

        private long BTreeNodeCount {
            get {
                return (IndexStream.Length / BTreeNode.TotalSize);
            }
        }

        public long Count {
            get {
                throw new NotImplementedException();
            }
        }

        public long WastedDataBytes {
            get {
                return _WastedDataBytes;
            }
        }

        private void ReadIndexEntry (long nodeIndex, uint valueIndex, out IndexEntry result) {
            using (var range = AccessBTreeValue(nodeIndex, valueIndex, MemoryMappedFileAccess.Read)) {
                var pEntry = (IndexEntry *)range.Pointer;
                result = *pEntry;
            }
        }

        private void ReadData (ref IndexEntry entry, out T value) {
            if (entry.KeyType == 0)
                throw new InvalidDataException();

            using (var range = DataStream.AccessRange(entry.DataOffset, entry.DataLength))
            using (var ms = new UnmanagedMemoryStream(range.Pointer, entry.DataLength, entry.DataLength, FileAccess.Read))
                Deserializer(ms, out value);
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

        private unsafe void InternalSetFoundValue (long nodeIndex, uint valueIndex, ref T value) {
            using (var range = AccessBTreeValue(nodeIndex, valueIndex, MemoryMappedFileAccess.Write)) {
                var pEntry = (IndexEntry *)range.Pointer;

                var segment = Serialize(ref value);
                uint count = (uint)segment.Count;

                long dataOffset = pEntry->DataOffset;
                WriteModes writeMode = (segment.Count > pEntry->DataLength) ?
                    WriteModes.AppendData : WriteModes.ReplaceData;

                if (writeMode == WriteModes.AppendData)
                    dataOffset = DataStream.AllocateSpace(count);

                var keyType = pEntry->KeyType;

                if (keyType == 0)
                    throw new InvalidDataException();

                pEntry->KeyType = 0;
                WriteData(ref *pEntry, ref segment, writeMode, dataOffset);
                pEntry->KeyType = keyType;
            }
        }

        private unsafe void WriteKey (ref IndexEntry indexEntry, ref TangleKey key) {
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
        private unsafe void WriteData (ref IndexEntry indexEntry, ref ArraySegment<byte> data, WriteModes writeMode, long? dataOffset) {
            if (indexEntry.KeyType != 0)
                throw new InvalidDataException();
            if (writeMode == WriteModes.Invalid)
                throw new InvalidDataException();

            var count = (uint)data.Count;

            if (writeMode == WriteModes.AppendData) {
                using (var range = DataStream.AccessRange(indexEntry.DataOffset, indexEntry.DataLength, MemoryMappedFileAccess.Write))
                    ZeroBytes(range.Pointer, 0, indexEntry.DataLength);

                _WastedDataBytes += indexEntry.DataLength;

                indexEntry.DataOffset = (uint)dataOffset.Value;
                indexEntry.DataLength = count;
            } else if (writeMode != WriteModes.ReplaceData) {
                if (dataOffset.HasValue)
                    indexEntry.DataOffset = (uint)dataOffset.Value;

                indexEntry.DataLength = count;
            }

            using (var range = DataStream.AccessRange(indexEntry.DataOffset, indexEntry.DataLength, MemoryMappedFileAccess.Write)) {
                WriteBytes(range.Pointer, 0, data);

                if (writeMode == WriteModes.ReplaceData) {
                    var bytesToZero = indexEntry.DataLength - count;
                    if (bytesToZero > 0)
                        ZeroBytes(range.Pointer, count, bytesToZero);

                    indexEntry.DataLength = count;
                }
            }
        }

        private unsafe bool InternalSet (TangleKey key, ref T value, IReplaceCallback replacementCallback) {
            WriteModes writeMode = WriteModes.Invalid;

            long keyOffset = 0;
            long? dataOffset = null;

            long nodeIndex, parentNodeIndex;
            uint valueIndex, parentValueIndex;

            bool foundExisting = FindKey(ref key, true, out nodeIndex, out valueIndex, out parentNodeIndex, out parentValueIndex);

            if (!foundExisting) {
                // Prepare BTree for insert
                BTreePrepareForInsert(nodeIndex, valueIndex);
            }

            using (var range = AccessBTreeValue(nodeIndex, valueIndex, MemoryMappedFileAccess.ReadWrite)) {
                var pEntry = (IndexEntry*)range.Pointer;

                if (foundExisting) {
                    if (pEntry->KeyType != key.OriginalTypeId)
                        throw new InvalidDataException();

                    bool shouldContinue = replacementCallback.ShouldReplace(this, ref *pEntry, ref value);
                    if (!shouldContinue) {
                        return false;
                    }

                    pEntry->KeyType = 0;
                }

                var segment = Serialize(ref value);

                if (foundExisting) {
                    if (segment.Count > pEntry->DataLength)
                        writeMode = WriteModes.AppendData;
                    else
                        writeMode = WriteModes.ReplaceData;
                } else {
                    writeMode = WriteModes.AppendDataAndKey;

                    if (key.Data.Count > IndexEntry.KeyPrefixSize)
                        keyOffset = KeyStream.AllocateSpace((uint)key.Data.Count);
                    else
                        keyOffset = 0;
                }

                if (writeMode != WriteModes.ReplaceData)
                    dataOffset = DataStream.AllocateSpace((uint)segment.Count);

                if (writeMode == WriteModes.AppendDataAndKey) {
                    *pEntry = new IndexEntry {
                        DataOffset = (uint)dataOffset.Value,
                        KeyOffset = (uint)keyOffset,
                        DataLength = (uint)segment.Count,
                        KeyLength = (ushort)key.Data.Count,
                        KeyType = 0
                    };

                    WriteKey(ref *pEntry, ref key);
                }

                WriteData(ref *pEntry, ref segment, writeMode, dataOffset);

                pEntry->KeyType = key.OriginalTypeId;
            }

            if (!foundExisting)
            using (var range = AccessBTreeNode(nodeIndex, MemoryMappedFileAccess.ReadWrite)) {
                // Finalize BTree after insert

                var pNode = (BTreeNode *)range.Pointer;
                if (pNode->IsValid != 0)
                    throw new InvalidDataException();
                if (pNode->HasLeaves != 0)
                    throw new InvalidDataException();

                pNode->IsValid = 1;
            }

            return true;
        }

        private struct NodeInfo {
            public long Node;
            public long? Parent;
        }

        /*
        private unsafe TangleKey GetKeyFromIndex (long index) {
            using (var indexRange = AccessIndex(index, MemoryMappedFileAccess.Read)) {
                var pEntry = (IndexEntry *)indexRange.Pointer;
                if (pEntry->KeyType == 0)
                    throw new InvalidDataException();

                using (var keyRange = KeyStream.AccessRange(
                    pEntry->KeyOffset, pEntry->KeyLength, MemoryMappedFileAccess.Read
                )) {
                    var array = new byte[pEntry->KeyLength];
                    ReadBytes(keyRange.Pointer, 0, array, 0, pEntry->KeyLength); 

                    return new TangleKey(array, pEntry->KeyType);
                }
            }
        }
         */

        private unsafe uint GetNodeValueCount (long nodeIndex) {
            using (var range = AccessBTreeNode(nodeIndex, MemoryMappedFileAccess.Read)) {
                var pNode = (BTreeNode *)range.Pointer;
                return pNode->NumValues;
            }
        }

        private unsafe string StringifyNode (long nodeIndex) {
            using (var range = AccessBTreeNode(nodeIndex, MemoryMappedFileAccess.Read))
                return StringifyNode((BTreeNode *)range.Pointer);
        }

        private unsafe string StringifyNode (BTreeNode * pNode) {
            var sb = new StringBuilder();
            var pValues = (IndexEntry *)(((byte *)pNode) + BTreeNode.OffsetOfValues);
            var pLeaves = (BTreeLeaf *)(((byte *)pNode) + BTreeNode.OffsetOfLeaves);

            for (int i = 0; i < pNode->NumValues; i++) {
                if (pNode->HasLeaves == 1)
                    sb.AppendFormat("{1} {2} ", pLeaves[i].NodeIndex, StringifyNode(pLeaves[i].NodeIndex), GetKeyOfEntry(&pValues[i]));
                else
                    sb.AppendFormat("{0} ", GetKeyOfEntry(&pValues[i]));
            }

            if (pNode->HasLeaves == 1)
                sb.AppendFormat("{1}", pLeaves[pNode->NumValues].NodeIndex, StringifyNode(pLeaves[pNode->NumValues].NodeIndex));

            return sb.ToString();
        }

        public string Stringify () {
            return StringifyNode(IndexStream.RootIndex);
        }

        private unsafe TangleKey GetKeyOfEntry (IndexEntry* pEntry) {
            byte[] buffer = new byte[pEntry->KeyLength];
            if (pEntry->KeyLength <= IndexEntry.KeyPrefixSize) {
                fixed (byte * pBuffer = buffer)
                    Native.memmove(pBuffer, pEntry->KeyPrefix, new UIntPtr(pEntry->KeyLength));
            } else {
                using (var keyRange = KeyStream.AccessRange(
                    pEntry->KeyOffset, pEntry->KeyLength, MemoryMappedFileAccess.Read
                ))
                    ReadBytes(keyRange.Pointer, 0, buffer, 0, pEntry->KeyLength);
            }
            return new TangleKey(buffer, pEntry->KeyType);
        }

        private unsafe TangleKey GetKeyOfNodeValue (long nodeIndex, uint valueIndex) {
            using (var range = AccessBTreeValue(nodeIndex, valueIndex, MemoryMappedFileAccess.Read)) {
                var pEntry = (IndexEntry *)range.Pointer;

                return GetKeyOfEntry(pEntry);
            }
        }

        public IEnumerable<TangleKey> Keys {
            get {
                // This should be a depth-first search of the tree...

                long count = BTreeNodeCount;
                for (long i = 0; i < count; i++) {
                    uint numValues = GetNodeValueCount(i);

                    for (uint j = 0; j < numValues; j++)
                        yield return GetKeyOfNodeValue(i, j);
                }
            }
        }

        private bool InternalFind (TangleKey key, out FindResult result) {
            long nodeIndex;
            uint valueIndex;

            if (!FindKey(ref key, out nodeIndex, out valueIndex)) {
                result = default(FindResult);
                return false;
            }

            result = new FindResult(this, ref key, nodeIndex, valueIndex);
            return true;
        }

        private bool InternalGet (TangleKey key, out T value) {
            long nodeIndex;
            uint valueIndex;

            if (!FindKey(ref key, out nodeIndex, out valueIndex)) {
                value = default(T);
                return false;
            }

            IndexEntry indexEntry;
            ReadIndexEntry(nodeIndex, valueIndex, out indexEntry);
            ReadData(ref indexEntry, out value);
            return true;
        }

        private unsafe void InternalGetFoundValue (long nodeIndex, uint valueIndex, out T result) {
            using (var range = AccessBTreeValue(nodeIndex, valueIndex, MemoryMappedFileAccess.Read)) {
                var pEntry = (IndexEntry*)range.Pointer;
                ReadData(ref *pEntry, out result);
            }
        }

        /// <summary>
        /// Exports the contents of each of the database's streams to a folder, for easier debugging.
        /// </summary>
        public void ExportStreams (string destinationFolder) {
            if (!Directory.Exists(destinationFolder))
                Directory.CreateDirectory(destinationFolder);

            ExportStream(IndexStream, Path.Combine(destinationFolder, "index"));
            ExportStream(KeyStream, Path.Combine(destinationFolder, "keys"));
            ExportStream(DataStream, Path.Combine(destinationFolder, "data"));
        }

        private void ExportStream (StreamRef stream, string file) {
            if (File.Exists(file))
                File.Delete(file);
            using (var fs = File.OpenWrite(file)) {
                var headerBuf = new byte[StreamRef.HeaderSize];
                using (var access = stream.AccessHeader())
                    Marshal.Copy(new IntPtr(access.Ptr), headerBuf, 0, (int)StreamRef.HeaderSize);
                fs.Write(headerBuf, 0, (int)StreamRef.HeaderSize);

                var size = (int)stream.Length;
                var buffer = new byte[size];
                using (var access = stream.AccessRange(0, (uint)size, MemoryMappedFileAccess.Read))
                    ReadBytes(access.Pointer, 0, buffer, 0, (uint)size);

                fs.Write(buffer, 0, size);
            }
        }

        public void Dispose () {
            IndexStream.Dispose();
            DataStream.Dispose();
            KeyStream.Dispose();

            if (OwnsStorage)
                Storage.Dispose();
        }
    }
}