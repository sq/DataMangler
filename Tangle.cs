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
        public byte KeyType, IsValid;
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
            if (TypeToTypeId.Count > 255)
                throw new InvalidOperationException("Too many registered types");

            var type = typeof(T);
            byte id = (byte)TypeToTypeId.Count;
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
                throw new InvalidOperationException("Key length limit exceeded");

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
                return Tangle.SetValueByIndex(Index, DataOffset, DataLength, newValue);
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

        private delegate bool ReplaceCallback (ref IndexEntry indexEntry, ref T newValue);

        private static readonly ReplaceCallback AlwaysReplace = _AlwaysReplace;
        private static readonly ReplaceCallback NeverReplace = _NeverReplace;

        public const string ExplanatoryPlaceholderText =
@"This is a DataMangler database. 
All the actual data is stored in NTFS streams attached to this file.
For more information, see http://support.microsoft.com/kb/105763.";

        public const bool TraceKeyInsertions = false;

        public const uint CurrentFormatVersion = 1;

        public readonly bool OwnsStorage;
        public readonly StreamSource Storage;
        public readonly TaskScheduler Scheduler;
        public readonly Serializer<T> Serializer;
        public readonly Deserializer<T> Deserializer;

        protected readonly ReaderWriterLockSlim IndexLock = new ReaderWriterLockSlim(LockRecursionPolicy.NoRecursion);

        protected struct WorkItem {
            public IFuture Future;
            public Action Action;
        }

        protected Squared.Task.Internal.WorkerThread<ConcurrentQueue<WorkItem>> _WorkerThread;

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
                WriteBytes(range, 0, data);

            return spot;
        }

        /// <summary>
        /// Reads a value from the tangle, looking it up via its key.
        /// </summary>
        /// <returns>A future that will contain the value once it has been read.</returns>
        /// <exception cref="KeyNotFoundException">If the specified key is not found, the future will contain a KeyNotFoundException.</exception>
        public Future<T> Get (TangleKey key) {
            var f = new Future<T>();
            QueueWorkItem(f, () => {
                T result;
                if (InternalGet(key, out result))
                    f.SetResult(result, null);
                else
                    f.SetResult(result, new KeyNotFoundException(key));
            });
            return f;
        }

        protected Future<T> GetValueByIndex (long index, long dataOffset, uint dataLength) {
            var f = new Future<T>();
            QueueWorkItem(f, () => {
                T result;
                InternalGetFoundValue(index, dataOffset, dataLength, out result);
                f.SetResult(result, null);
            });
            return f;
        }

        protected IFuture SetValueByIndex (long index, long existingOffset, uint existingLength, T value) {
            var f = new SignalFuture();
            QueueWorkItem(f, () => {
                InternalSetFoundValue(index, existingOffset, existingLength, value);
                f.Complete();
            });
            return f;
        }

        /// <summary>
        /// Searches the tangle for a given key, and if it is found, returns a reference to the key that can be used to retrieve or replace its associated value.
        /// </summary>
        /// <returns>A future that will contain a reference to the key, if it was found.</returns>
        /// <exception cref="KeyNotFoundException">If the specified key is not found, the future will contain a KeyNotFoundException.</exception>
        public Future<FindResult> Find (TangleKey key) {
            var f = new Future<FindResult>();
            QueueWorkItem(f, () => {
                FindResult result;
                if (InternalFind(key, out result))
                    f.SetResult(result, null);
                else
                    f.SetResult(result, new KeyNotFoundException(key));
            });
            return f;
        }

        /// <summary>
        /// Stores a value into the tangle, assigning it a given key. If the given key already has an associated value, that value is replaced.
        /// </summary>
        /// <returns>A future that completes once the value has been stored to disk.</returns>
        public IFuture Set (TangleKey key, T value) {
            var f = new SignalFuture();
            QueueWorkItem(f, () => {
                InternalSet(key, value, AlwaysReplace);
                f.Complete();
            });
            return f;
        }

        private static bool _AlwaysReplace (ref IndexEntry indexEntry, ref T newValue) {
            return true;
        }

        private static bool _NeverReplace (ref IndexEntry indexEntry, ref T newValue) {
            return false;
        }

        /// <summary>
        /// Stores a value into the tangle, assigning it a given key. If the given key already has an associated value, the operation will abort.
        /// </summary>
        /// <returns>A future that completes once the value has been stored to disk. The future's value will be false if the operation was aborted.</returns>
        public Future<bool> Add (TangleKey key, T value) {
            var f = new Future<bool>();
            QueueWorkItem(f, () => {
                f.SetResult(InternalSet(key, value, NeverReplace), null);
            });
            return f;
        }

        /// <summary>
        /// Stores a value into the tangle, assigning it a given key. If the given key already has an associated value, a callback is invoked to determine the new value for the key.
        /// </summary>
        /// <returns>A future that completes once the value has been stored to disk.</returns>
        public Future<bool> AddOrUpdate (TangleKey key, T value, UpdateCallback updateCallback) {
            ReplaceCallback replaceCallback = 
                (ref IndexEntry indexEntry, ref T newValue) => {
                    T oldValue;
                    ReadValue(ref indexEntry, out oldValue);
                    newValue = updateCallback(oldValue);
                    return true;
                };

            var f = new Future<bool>();
            QueueWorkItem(f, () => {
                f.SetResult(InternalSet(key, value, replaceCallback), null);
            });
            return f;
        }

        /// <summary>
        /// Stores a value into the tangle, assigning it a given key. If the given key already has an associated value, a callback is invoked to determine the new value for the key.
        /// </summary>
        /// <returns>A future that completes once the value has been stored to disk.</returns>
        public Future<bool> AddOrUpdate (TangleKey key, T value, DecisionUpdateCallback updateCallback) {
            ReplaceCallback replaceCallback =
                (ref IndexEntry indexEntry, ref T newValue) => {
                    ReadValue(ref indexEntry, out newValue);
                    return updateCallback(ref newValue);
                };

            var f = new Future<bool>();
            QueueWorkItem(f, () => {
                f.SetResult(InternalSet(key, value, replaceCallback), null);
            });
            return f;
        }

        private void QueueWorkItem (IFuture future, Action action) {
            if (_WorkerThread == null)
                _WorkerThread = new Squared.Task.Internal.WorkerThread<ConcurrentQueue<WorkItem>>(WorkerThreadFunc, ThreadPriority.Normal);

            _WorkerThread.WorkItems.Enqueue(new WorkItem {
                Future = future,
                Action = action
            });

            _WorkerThread.Wake();
        }

        protected void WorkerThreadFunc (ConcurrentQueue<WorkItem> workItems, ManualResetEvent newWorkItemEvent) {
            while (true) {
                WorkItem item;
                while (workItems.TryDequeue(out item)) {
                    try {
                        item.Action();
                    } catch (Exception ex) {
                        item.Future.Fail(ex);
                    }
                }

                newWorkItemEvent.WaitOne();
                newWorkItemEvent.Reset();
            }
        }

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
                    if (pEntry->IsValid != 1)
                        throw new InvalidDataException();

                    using (var keyRange = KeyStream.AccessRange(
                        pEntry->KeyOffset, pEntry->KeyLength, MemoryMappedFileAccess.Read
                    )) {
                        var pLhs = keyRange.Pointer;
                        delta = CompareKeys(pLhs, pEntry->KeyLength, pRhs, lengthRhs);
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

        private long IndexCount {
            get {
                return (IndexStream.Length / IndexEntry.Size);
            }
        }

        private bool IndexEntryByKey (ref TangleKey key, out IndexEntry resultEntry, out long resultIndex) {
            return IndexEntryByKey(0, IndexCount, ref key, out resultEntry, out resultIndex);
        }

        private void ReadValue (ref IndexEntry entry, out T value) {
            using (var range = DataStream.AccessRange(entry.DataOffset, entry.DataLength))
            using (var ms = new UnmanagedMemoryStream(range.Pointer, entry.DataLength, entry.DataLength, FileAccess.Read))
                Deserializer(ms, out value);
        }

        private static void ReadBytes (StreamRange range, long offset, byte[] buffer, long bufferOffset, uint count) {
            var ptr = range.Pointer;
            for (uint i = 0; i < count; i++)
                buffer[i + bufferOffset] = ptr[i + offset];
        }

        private static void WriteBytes (StreamRange range, long offset, ArraySegment<byte> bytes) {
            var ptr = range.Pointer;
            for (uint i = 0; i < bytes.Count; i++)
                ptr[i + offset] = bytes.Array[i + bytes.Offset];
        }

        private static void ZeroBytes (StreamRange range, long offset, uint count) {
            var ptr = range.Pointer;
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
                var ptr = range.Pointer;

                while (position >= 0) {
                    IndexEntry* pSource = (IndexEntry *)(ptr + position);
                    IndexEntry* pDest = (IndexEntry*)(ptr + position + IndexEntry.Size);

                    if (pSource->IsValid != 1)
                        throw new InvalidDataException();
                    pSource->IsValid = 0;

                    *pDest = *pSource;

                    pDest->IsValid = 1;

                    position -= IndexEntry.Size;
                }
            }
        }

        private unsafe void InternalSetFoundValue (long index, long existingOffset, uint existingLength, T value) {
            if ((index < 0) || (index >= IndexCount))
                throw new IndexOutOfRangeException();

            using (var ms = new MemoryStream()) {
                Serializer(ref value, ms);

                var segment = ms.GetSegment();
                uint count = (uint)segment.Count;

                long dataOffset = existingOffset;
                WriteModes writeMode = (ms.Length > existingLength) ?
                    WriteModes.AppendData : WriteModes.ReplaceData;

                if (writeMode == WriteModes.AppendData)
                    dataOffset = DataStream.AllocateSpace(count);

                using (var range = AccessIndex(index)) {
                    var pEntry = (IndexEntry*)range.Pointer;

                    if ((pEntry->IsValid != 1) ||
                        (pEntry->DataOffset != existingOffset) || 
                        (pEntry->DataLength != existingLength))
                        throw new InvalidDataException();

                    pEntry->IsValid = 0;
                    WriteData(ref *pEntry, ref segment, writeMode, dataOffset);
                    pEntry->IsValid = 1;
                }
            }
        }

        private unsafe void WriteKey (ref IndexEntry indexEntry, ref TangleKey key) {
            using (var keyRange = KeyStream.AccessRange(indexEntry.KeyOffset, indexEntry.KeyLength, MemoryMappedFileAccess.Write))
                WriteBytes(keyRange, 0, key.KeyData);
        }

        // IndexEntry must be fully prepared for the write operation:
        //  KeyOffset/KeyLength must be filled in.
        //  DataOffset/DataLength must be filled in.
        //  The IndexEntry's IsValid must be 0.
        // newOffset must specify the offset within the data stream where the data is to be written.
        //  In most cases this should be equal to DataOffset, but in the case of AppendData it will be different.
        private unsafe void WriteData (ref IndexEntry indexEntry, ref ArraySegment<byte> data, WriteModes writeMode, long dataOffset) {
            if (indexEntry.IsValid != 0)
                throw new InvalidDataException();
            if (writeMode == WriteModes.Invalid)
                throw new InvalidDataException();

            var count = (uint)data.Count;

            if (writeMode == WriteModes.AppendData) {
                using (var range = DataStream.AccessRange(indexEntry.DataOffset, indexEntry.DataLength, MemoryMappedFileAccess.Write))
                    ZeroBytes(range, 0, indexEntry.DataLength);
            }
            
            indexEntry.DataOffset = (uint)dataOffset;
            if (writeMode != WriteModes.ReplaceData)
                indexEntry.DataLength = count;

            using (var range = DataStream.AccessRange(indexEntry.DataOffset, indexEntry.DataLength, MemoryMappedFileAccess.Write)) {
                WriteBytes(range, 0, data);

                if (writeMode == WriteModes.ReplaceData) {
                    var bytesToZero = indexEntry.DataLength - count;
                    if (bytesToZero > 0)
                        ZeroBytes(range, count, bytesToZero);

                    indexEntry.DataLength = count;
                }
            }

        }

        private unsafe bool InternalSet (TangleKey key, T value, ReplaceCallback replacementCallback) {
            WriteModes writeMode = WriteModes.Invalid;
            IndexEntry indexEntry;
            long offset = 0, dataOffset = 0, keyOffset = 0;
            long positionToClear = 0, emptyPosition = 0;
            long index, count = IndexCount;

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
                if (replacementCallback == NeverReplace)
                    return false;

                if (replacementCallback != AlwaysReplace) {
                    bool shouldContinue = replacementCallback(ref indexEntry, ref value);
                    if (!shouldContinue)
                        return false;
                }
            }

            using (var ms = new MemoryStream()) {
                Serializer(ref value, ms);

                if (foundExisting) {
                    if (ms.Length > indexEntry.DataLength)
                        writeMode = WriteModes.AppendData;
                    else
                        writeMode = WriteModes.ReplaceData;
                }

                if (writeMode == WriteModes.Invalid)
                    throw new InvalidDataException();

                if (writeMode == WriteModes.InsertIndex || writeMode == WriteModes.AppendIndex)
                    keyOffset = KeyStream.AllocateSpace((uint)key.KeyData.Count);

                if (writeMode != WriteModes.ReplaceData)
                    dataOffset = DataStream.AllocateSpace((uint)ms.Length);

                if (writeMode == WriteModes.InsertIndex)
                    MoveEntriesForward(positionToClear, emptyPosition);

                using (var indexRange = IndexStream.AccessRange(offset, IndexEntry.Size, MemoryMappedFileAccess.ReadWrite)) {
                    var pEntry = (IndexEntry *)indexRange.Pointer;

                    if (writeMode == WriteModes.AppendIndex || writeMode == WriteModes.InsertIndex) {
                        *pEntry = new IndexEntry {
                            DataOffset = (uint)dataOffset,
                            KeyOffset = (uint)keyOffset,
                            DataLength = (uint)ms.Length,
                            KeyLength = (ushort)key.KeyData.Count,
                            KeyType = key.OriginalTypeId,
                            IsValid = 0
                        };

                        WriteKey(ref *pEntry, ref key);
                    } else {
                        pEntry->IsValid = 0;
                    }

                    var segment = ms.GetSegment();
                    WriteData(ref *pEntry, ref segment, writeMode, dataOffset);

                    pEntry->IsValid = 1;
                }
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
                if (pEntry->IsValid != 1)
                    throw new InvalidDataException();

                using (var keyRange = KeyStream.AccessRange(
                    pEntry->KeyOffset, pEntry->KeyLength, MemoryMappedFileAccess.Read
                )) {
                    var array = new byte[pEntry->KeyLength];
                    ReadBytes(keyRange, 0, array, 0, pEntry->KeyLength); 

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

            ReadValue(ref indexEntry, out value);
            return true;
        }

        private unsafe void InternalGetFoundValue (long index, long dataOffset, long dataLength, out T result) {
            if ((index < 0) || (index >= IndexCount))
                throw new IndexOutOfRangeException();

            using (var range = AccessIndex(index)) {
                var pEntry = (IndexEntry*)range.Pointer;
                ReadValue(ref *pEntry, out result);
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
                    ReadBytes(access, 0, buffer, 0, (uint)size);
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