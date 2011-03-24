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
using Squared.Data.Mangler.Serialization;
using Squared.Task;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading;
using Squared.Data.Mangler.Internal;
using System.Xml.Serialization;
using System.IO.MemoryMappedFiles;
using Squared.Util;
using System.Collections.Concurrent;

namespace Squared.Data.Mangler.Internal {
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    internal unsafe struct IndexEntry {
        public static readonly uint Size;

        static IndexEntry () {
            Size = (uint)Marshal.SizeOf(typeof(IndexEntry));
        }

        public uint KeyOffset, DataOffset;
        public ushort KeyLength;
        public uint DataLength;
        public byte KeyType;
    }
}

namespace Squared.Data.Mangler {
    public class KeyNotFoundException : Exception {
        public readonly TangleKey Key;

        public KeyNotFoundException (TangleKey key)
            : base("The specified key was not found in the tangle.") {
            Key = key;
        }
    }

    public struct TangleKey {
        private static readonly Dictionary<byte, Type> TypeIdToType = new Dictionary<byte, Type>();
        private static readonly Dictionary<Type, byte> TypeToTypeId = new Dictionary<Type, byte>();

        static TangleKey () {
            RegisterType<string>();
            RegisterType<byte[]>();
            RegisterType<uint>();
            RegisterType<int>();
            RegisterType<ulong>();
            RegisterType<long>();
        }

        private static void RegisterType<T> () {
            if (TypeToTypeId.Count > 254)
                throw new InvalidOperationException("Too many registered types");

            var type = typeof(T);
            byte id = (byte)(TypeToTypeId.Count + 1);
            TypeToTypeId[type] = id;
            TypeIdToType[id] = type;
        }

        public readonly byte OriginalTypeId;
        public readonly ArraySegment<byte> KeyData;

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

        public TangleKey (byte[] array, byte originalType)
            : this(array, 0, array.Length, originalType) {
        }

        public TangleKey (byte[] array, int offset, int count, byte originalType) {
            if (count >= ushort.MaxValue)
                throw new InvalidDataException("Key too long");
            if (originalType == 0)
                throw new InvalidDataException("Invalid key type");

            KeyData = new ArraySegment<byte>(array, offset, count);
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
                    return Encoding.ASCII.GetString(KeyData.Array, KeyData.Offset, KeyData.Count);
                } else if (type == typeof(int)) {
                    return BitConverter.ToInt32(KeyData.Array, KeyData.Offset);
                } else if (type == typeof(uint)) {
                    return BitConverter.ToUInt32(KeyData.Array, KeyData.Offset);
                } else if (type == typeof(long)) {
                    return BitConverter.ToInt64(KeyData.Array, KeyData.Offset);
                } else if (type == typeof(ulong)) {
                    return BitConverter.ToUInt64(KeyData.Array, KeyData.Offset);
                } else /* if (type == typeof(byte[])) */ {
                    return KeyData;
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
                for (int i = 0; i < KeyData.Count; i++)
                    sb.AppendFormat("{0:X2}", KeyData.Array[i + KeyData.Offset]);
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
            AppendIndex, // Append new index entry, write new key and data
            InsertIndex, // Insert new index entry, write new key and data
            ReplaceData, // Write data over existing data, update index
            AppendData,   // Append new data, erase existing data, update index
        }


        public struct FindResult {
            public readonly Tangle<T> Tangle;
            public readonly TangleKey Key;
            private readonly long Index, DataOffset;
            private readonly uint DataLength;

            internal FindResult (Tangle<T> owner, ref TangleKey key, long index, long dataOffset, uint dataLength) {
                Tangle = owner;
                Key = key;
                Index = index;
                DataOffset = dataOffset;
                DataLength = dataLength;
            }

            public Future<T> GetValue () {
                return Tangle.GetValueByIndex(Index, DataOffset, DataLength);
            }

            public IFuture SetValue (T newValue) {
                return Tangle.SetValueByIndex(Index, DataOffset, DataLength, ref newValue);
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

        private class SetThunk : IWorkItem<T>, IReplaceCallback {
            public readonly Future<bool> Future = new Future<bool>();
            public readonly TangleKey Key;
            public T Value;
            public readonly bool ShouldReplace;

            public SetThunk (ref TangleKey key, ref T value, bool shouldReplace) {
                Key = key;
                Value = value;
                ShouldReplace = shouldReplace;
            }

            bool IReplaceCallback.ShouldReplace (Tangle<T> tangle, ref IndexEntry indexEntry, ref T newValue) {
                return ShouldReplace;
            }

            public void Execute (Tangle<T> tangle) {
                try {
                    Future.SetResult(tangle.InternalSet(Key, ref Value, this), null);
                } catch (Exception ex) {
                    Future.SetResult(false, ex);
                }
            }
        }

        private class UpdateThunk : IWorkItem<T>, IReplaceCallback {
            public readonly Future<bool> Future = new Future<bool>();
            public readonly TangleKey Key;
            public T Value;
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

            public void Execute (Tangle<T> tangle) {
                try {
                    Future.SetResult(tangle.InternalSet(Key, ref Value, this), null);
                } catch (Exception ex) {
                    Future.SetResult(false, ex);
                }
            }
        }

        private class GetThunk : IWorkItem<T> {
            public readonly Future<T> Future = new Future<T>();
            public readonly TangleKey Key;

            public GetThunk (ref TangleKey key) {
                Key = key;
            }

            public void Execute (Tangle<T> tangle) {
                try {
                    T result;
                    if (tangle.InternalGet(Key, out result))
                        Future.SetResult(result, null);
                    else
                        Future.SetResult(result, new KeyNotFoundException(Key));
                } catch (Exception ex) {
                    Future.SetResult(default(T), ex);
                }
            }
        }

        private class FindThunk : IWorkItem<T> {
            public readonly Future<FindResult> Future = new Future<FindResult>();
            public readonly TangleKey Key;

            public FindThunk (ref TangleKey key) {
                Key = key;
            }

            public void Execute (Tangle<T> tangle) {
                try {
                    FindResult result;
                    if (tangle.InternalFind(Key, out result))
                        Future.SetResult(result, null);
                    else
                        Future.SetResult(result, new KeyNotFoundException(Key)); 
                } catch (Exception ex) {
                    Future.Fail(ex);
                }
            }
        }

        private class GetByIndexThunk : IWorkItem<T> {
            public readonly Future<T> Future = new Future<T>();
            public readonly long Index;
            public readonly long DataOffset;
            public readonly uint DataLength;

            public GetByIndexThunk (long index, long dataOffset, uint dataLength) {
                Index = index;
                DataOffset = dataOffset;
                DataLength = dataLength;
            }

            public void Execute (Tangle<T> tangle) {
                try {
                    T result;
                    tangle.InternalGetFoundValue(Index, DataOffset, DataLength, out result);
                    Future.SetResult(result, null);
                } catch (Exception ex) {
                    Future.SetResult(default(T), ex);
                }
            }
        }

        private class SetByIndexThunk : IWorkItem<T> {
            public readonly SignalFuture Future = new SignalFuture();
            public readonly long Index;
            public readonly long DataOffset;
            public readonly uint DataLength;
            public T Value;

            public SetByIndexThunk (long index, long dataOffset, uint dataLength, ref T value) {
                Index = index;
                DataOffset = dataOffset;
                DataLength = dataLength;
                Value = value;
            }

            public void Execute (Tangle<T> tangle) {
                try {
                    tangle.InternalSetFoundValue(Index, DataOffset, DataLength, ref Value);
                    Future.Complete();
                } catch (Exception ex) {
                    Future.Fail(ex);
                }
            }
        }


        public const bool TraceKeyInsertions = false;
        public const uint CurrentFormatVersion = 1;
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
            var thunk = new GetThunk(ref key);
            QueueWorkItem(thunk);
            return thunk.Future;
        }

        protected Future<T> GetValueByIndex (long index, long dataOffset, uint dataLength) {
            var thunk = new GetByIndexThunk(index, dataOffset, dataLength);
            QueueWorkItem(thunk);
            return thunk.Future;
        }

        protected IFuture SetValueByIndex (long index, long dataOffset, uint dataLength, ref T value) {
            var thunk = new SetByIndexThunk(index, dataOffset, dataLength, ref value);
            QueueWorkItem(thunk);
            return thunk.Future;
        }

        /// <summary>
        /// Searches the tangle for a given key, and if it is found, returns a reference to the key that can be used to retrieve or replace its associated value.
        /// </summary>
        /// <returns>A future that will contain a reference to the key, if it was found.</returns>
        /// <exception cref="KeyNotFoundException">If the specified key is not found, the future will contain a KeyNotFoundException.</exception>
        public Future<FindResult> Find (TangleKey key) {
            var thunk = new FindThunk(ref key);
            QueueWorkItem(thunk);
            return thunk.Future;
        }

        /// <summary>
        /// Stores a value into the tangle, assigning it a given key. If the given key already has an associated value, that value is replaced.
        /// </summary>
        /// <returns>A future that completes once the value has been stored to disk.</returns>
        public IFuture Set (TangleKey key, T value) {
            var thunk = new SetThunk(ref key, ref value, true);
            QueueWorkItem(thunk);
            return thunk.Future;
        }

        /// <summary>
        /// Stores a value into the tangle, assigning it a given key. If the given key already has an associated value, the operation will abort.
        /// </summary>
        /// <returns>A future that completes once the value has been stored to disk. The future's value will be false if the operation was aborted.</returns>
        public Future<bool> Add (TangleKey key, T value) {
            var thunk = new SetThunk(ref key, ref value, false);
            QueueWorkItem(thunk);
            return thunk.Future;
        }

        /// <summary>
        /// Stores a value into the tangle, assigning it a given key. If the given key already has an associated value, a callback is invoked to determine the new value for the key.
        /// </summary>
        /// <returns>A future that completes once the value has been stored to disk.</returns>
        public Future<bool> AddOrUpdate (TangleKey key, T value, UpdateCallback updateCallback) {
            var thunk = new UpdateThunk(ref key, ref value, updateCallback);
            QueueWorkItem(thunk);
            return thunk.Future;
        }

        /// <summary>
        /// Stores a value into the tangle, assigning it a given key. If the given key already has an associated value, a callback is invoked to determine the new value for the key.
        /// </summary>
        /// <returns>A future that completes once the value has been stored to disk.</returns>
        public Future<bool> AddOrUpdate (TangleKey key, T value, DecisionUpdateCallback updateCallback) {
            var thunk = new UpdateThunk(ref key, ref value, updateCallback);
            QueueWorkItem(thunk);
            return thunk.Future;
        }

        private void QueueWorkItem (IWorkItem<T> workItem) {
            if (_WorkerThread == null)
                _WorkerThread = new Squared.Task.Internal.WorkerThread<ConcurrentQueue<IWorkItem<T>>>(
                    WorkerThreadFunc, ThreadPriority.Normal, String.Format("Tangle<{0}> Worker", typeof(T).ToString())
                );

            _WorkerThread.WorkItems.Enqueue(workItem);

            _WorkerThread.Wake();
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

        /*
        private unsafe int CompareKeys (byte * lhs, uint lengthLhs, byte * rhs, uint lengthRhs) {
            uint compareLength = Math.Min(lengthLhs, lengthRhs);

            for (uint i = 0; i < compareLength; i++) {
                int delta = lhs[i] - rhs[i];
                if (delta != 0)
                    return Math.Sign(delta);
            }

            if (lengthLhs > compareLength)
                return 1;
            else if (lengthRhs > compareLength)
                return -1;
            else
                return 0;
        }
         */

        private StreamRange AccessIndex (long index) {
            long count = (IndexStream.Length / IndexEntry.Size);

            if ((index < 0) || (index >= count))
                throw new ArgumentException(String.Format(
                    "Expected 0 <= index < {0}, but index was {1}", count, index
                ), "index");

            long position = index * IndexEntry.Size;
            return IndexStream.AccessRange(position, IndexEntry.Size, MemoryMappedFileAccess.Read);
        }

        private unsafe bool IndexEntryByKey (long firstIndex, long indexCount, ref TangleKey rhs, out IndexEntry resultEntry, out long resultIndex) {
            uint lengthRhs = (uint)rhs.KeyData.Count;
            long min = firstIndex, max = firstIndex + indexCount - 1;
            long pivot;
            int delta = 0;

            fixed (byte * pRhs = &rhs.KeyData.Array[rhs.KeyData.Offset])
            while (min <= max) {

                pivot = min + ((max - min) >> 1);

                using (var indexRange = AccessIndex(pivot)) {
                    var pEntry = (IndexEntry *)indexRange.Pointer;
                    if (pEntry->KeyType == 0)
                        throw new InvalidDataException();

                    using (var keyRange = KeyStream.AccessRange(
                        pEntry->KeyOffset, pEntry->KeyLength, MemoryMappedFileAccess.Read
                    )) {
                        var pLhs = keyRange.Pointer;

                        var compareLength = Math.Min(pEntry->KeyLength, lengthRhs);
                        delta = Native.memcmp(pLhs, pRhs, new UIntPtr(compareLength));
                        if (delta == 0) {
                            if (pEntry->KeyLength > compareLength)
                                delta = 1;
                            else if (lengthRhs > compareLength)
                                delta = -1;
                        }
                    }

                    if (delta == 0) {
                        resultEntry = *pEntry;
                        resultIndex = pivot;

                        return true;
                    } else if (delta < 0) {
                        min = pivot + 1;
                    } else {
                        max = pivot - 1;
                    }
                }
            }

            resultEntry = default(IndexEntry);
            resultIndex = min;

            return false;
        }

        public long Count {
            get {
                return (IndexStream.Length / IndexEntry.Size);
            }
        }

        public long WastedDataBytes {
            get {
                return _WastedDataBytes;
            }
        }

        private bool IndexEntryByKey (ref TangleKey key, out IndexEntry resultEntry, out long resultIndex) {
            return IndexEntryByKey(0, Count, ref key, out resultEntry, out resultIndex);
        }

        private void ReadData (ref IndexEntry entry, out T value) {
            if (entry.KeyType == 0)
                throw new InvalidDataException();

            using (var range = DataStream.AccessRange(entry.DataOffset, entry.DataLength))
            using (var ms = new UnmanagedMemoryStream(range.Pointer, entry.DataLength, entry.DataLength, FileAccess.Read))
                Deserializer(ms, out value);
        }

        private static void ReadBytes (byte * ptr, long offset, byte[] buffer, long bufferOffset, uint count) {
            for (uint i = 0; i < count; i++)
                buffer[i + bufferOffset] = ptr[i + offset];
        }

        private static void WriteBytes (byte* ptr, long offset, ArraySegment<byte> bytes) {
            for (uint i = 0; i < bytes.Count; i++)
                ptr[i + offset] = bytes.Array[i + bytes.Offset];
        }

        private static void ZeroBytes (byte* ptr, long offset, uint count) {
            for (uint i = 0; i < count; i++)
                ptr[i + offset] = 0;
        }

        private unsafe void MoveEntriesForward (long positionToClear, long emptyPosition) {
            long size = emptyPosition - positionToClear + IndexEntry.Size;
            long position = size - (IndexEntry.Size * 2);

            using (var range = IndexStream.AccessRange(
                positionToClear, (uint)size,
                MemoryMappedFileAccess.ReadWrite
            )) {
                Native.memmove(range.Pointer + IndexEntry.Size, range.Pointer, new UIntPtr((ulong)(size - IndexEntry.Size)));
                /*
                var ptr = range.Pointer;

                while (position >= 0) {
                    IndexEntry* pSource = (IndexEntry *)(ptr + position);
                    IndexEntry* pDest = (IndexEntry*)(ptr + position + IndexEntry.Size);

                    var kt = pSource->KeyType;
                    if (kt == 0)
                        throw new InvalidDataException();

                    pSource->KeyType = 0;

                    *pDest = *pSource;

                    pDest->KeyType = kt;

                    position -= IndexEntry.Size;
                }
                 */
            }
        }

        private unsafe void InternalSetFoundValue (long index, long existingOffset, uint existingLength, ref T value) {
            if ((index < 0) || (index >= Count))
                throw new IndexOutOfRangeException();

            var segment = Serialize(ref value);
            uint count = (uint)segment.Count;

            long dataOffset = existingOffset;
            WriteModes writeMode = (segment.Count > existingLength) ?
                WriteModes.AppendData : WriteModes.ReplaceData;

            if (writeMode == WriteModes.AppendData)
                dataOffset = DataStream.AllocateSpace(count);

            using (var range = AccessIndex(index)) {
                var pEntry = (IndexEntry*)range.Pointer;

                var kt = pEntry->KeyType;

                if ((kt == 0) ||
                    (pEntry->DataOffset != existingOffset) || 
                    (pEntry->DataLength != existingLength))
                    throw new InvalidDataException();

                pEntry->KeyType = 0;
                WriteData(ref *pEntry, ref segment, writeMode, dataOffset);
                pEntry->KeyType = kt;
            }
        }

        private unsafe void WriteKey (ref IndexEntry indexEntry, ref TangleKey key) {
            using (var keyRange = KeyStream.AccessRange(indexEntry.KeyOffset, indexEntry.KeyLength, MemoryMappedFileAccess.Write))
                WriteBytes(keyRange.Pointer, 0, key.KeyData);
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
            IndexEntry indexEntry;
            long offset = 0, keyOffset = 0;
            long? dataOffset = null;
            long positionToClear = 0, emptyPosition = 0;
            long index, count = Count;

            bool foundExisting = IndexEntryByKey(0, count, ref key, out indexEntry, out index);
            offset = index * IndexEntry.Size;

            if (!foundExisting) {
                var newSpot = IndexStream.AllocateSpace(IndexEntry.Size);

                if (index < count) {
                    writeMode = WriteModes.InsertIndex;
                    positionToClear = offset;
                    emptyPosition = newSpot;
                } else {
                    writeMode = WriteModes.AppendIndex;
                    offset = newSpot;
                }
            } else {
                bool shouldContinue = replacementCallback.ShouldReplace(this, ref indexEntry, ref value);
                if (!shouldContinue)
                    return false;
            }

            var segment = Serialize(ref value);

            if (foundExisting) {
                if (segment.Count > indexEntry.DataLength)
                    writeMode = WriteModes.AppendData;
                else
                    writeMode = WriteModes.ReplaceData;
            }

            if (writeMode == WriteModes.Invalid)
                throw new InvalidDataException();

            if (writeMode == WriteModes.InsertIndex || writeMode == WriteModes.AppendIndex)
                keyOffset = KeyStream.AllocateSpace((uint)key.KeyData.Count);

            if (writeMode != WriteModes.ReplaceData)
                dataOffset = DataStream.AllocateSpace((uint)segment.Count);

            if (writeMode == WriteModes.InsertIndex)
                MoveEntriesForward(positionToClear, emptyPosition);

            using (var indexRange = IndexStream.AccessRange(offset, IndexEntry.Size, MemoryMappedFileAccess.ReadWrite)) {
                var pEntry = (IndexEntry *)indexRange.Pointer;

                if (writeMode == WriteModes.AppendIndex || writeMode == WriteModes.InsertIndex) {
                    *pEntry = new IndexEntry {
                        DataOffset = (uint)dataOffset.Value,
                        KeyOffset = (uint)keyOffset,
                        DataLength = (uint)segment.Count,
                        KeyLength = (ushort)key.KeyData.Count,
                        KeyType = 0
                    };

                    WriteKey(ref *pEntry, ref key);
                } else {
                    pEntry->KeyType = 0;
                }

                WriteData(ref *pEntry, ref segment, writeMode, dataOffset);

                pEntry->KeyType = key.OriginalTypeId;
            }

            if (TraceKeyInsertions) {
                Console.WriteLine("--------");
                foreach (var k in Keys)
                    Console.WriteLine(k.Value);
            }

            return true;
        }

        private unsafe TangleKey GetKeyFromIndex (long index) {
            using (var indexRange = AccessIndex(index)) {
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

        public IEnumerable<TangleKey> Keys {
            get {
                long count = IndexStream.Length / IndexEntry.Size;
                for (long i = 0; i < count; i++)
                    yield return GetKeyFromIndex(i);
            }
        }

        private bool InternalFind (TangleKey key, out FindResult result) {
            IndexEntry indexEntry;
            long index;

            if (!IndexEntryByKey(ref key, out indexEntry, out index)) {
                result = default(FindResult);
                return false;
            }

            result = new FindResult(this, ref key, index, indexEntry.DataOffset, indexEntry.DataLength);
            return true;
        }

        private bool InternalGet (TangleKey key, out T value) {
            IndexEntry indexEntry;
            long temp;

            if (!IndexEntryByKey(ref key, out indexEntry, out temp)) {
                value = default(T);
                return false;
            }

            ReadData(ref indexEntry, out value);
            return true;
        }

        private unsafe void InternalGetFoundValue (long index, long dataOffset, long dataLength, out T result) {
            if ((index < 0) || (index >= Count))
                throw new IndexOutOfRangeException();

            using (var range = AccessIndex(index)) {
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