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
using Squared.Task;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading;
using Squared.Data.Mangler.Internal;
using System.Xml.Serialization;
using System.IO.MemoryMappedFiles;

namespace Squared.Data.Mangler.Internal {
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    internal unsafe struct IndexEntry {
        public long KeyOffset, DataOffset;
        public uint KeyLength, DataLength;
        public int IsValid;
    }
}

namespace Squared.Data.Mangler {
    /// <summary>
    /// Handles converting a single value from the Tangle into raw binary for storage.
    /// </summary>
    public delegate void TangleSerializer<T> (ref T input, Stream output);
    /// <summary>
    /// Handles converting a single stored value from raw binary back into its native format, when the Tangle is loading it from storage.
    /// </summary>
    public delegate void TangleDeserializer<T> (Stream input, out T output);

    public struct TangleKey {
        private readonly Type OriginalType;
        public readonly ArraySegment<byte> KeyData;

        public TangleKey (uint key)
            : this(BitConverter.GetBytes(key)) {
            OriginalType = typeof(uint);
        }

        public TangleKey (ulong key)
            : this(BitConverter.GetBytes(key)) {
            OriginalType = typeof(ulong);
        }

        public TangleKey (int key)
            : this(BitConverter.GetBytes(key)) {
            OriginalType = typeof(int);
        }

        public TangleKey (long key)
            : this(BitConverter.GetBytes(key)) {
            OriginalType = typeof(long);
        }

        public TangleKey (string key)
            : this (Encoding.ASCII.GetBytes(key)) {
            OriginalType = typeof(string);
        }

        public TangleKey (byte[] array)
            : this(array, 0, array.Length) {
        }

        public TangleKey (byte[] array, int offset, int count) {
            KeyData = new ArraySegment<byte>(array, offset, count);
            OriginalType = typeof(byte[]);
        }

        public static implicit operator TangleKey (string key) {
            return new TangleKey(key);
        }

        public override string ToString () {
            if (OriginalType == typeof(string)) {
                return Encoding.ASCII.GetString(KeyData.Array, KeyData.Offset, KeyData.Count);
            } else if (OriginalType == typeof(int)) {
                return BitConverter.ToInt32(KeyData.Array, KeyData.Offset).ToString();
            } else if (OriginalType == typeof(uint)) {
                return BitConverter.ToUInt32(KeyData.Array, KeyData.Offset).ToString();
            } else if (OriginalType == typeof(long)) {
                return BitConverter.ToInt64(KeyData.Array, KeyData.Offset).ToString();
            } else if (OriginalType == typeof(ulong)) {
                return BitConverter.ToUInt64(KeyData.Array, KeyData.Offset).ToString();
            } else {
                var sb = new StringBuilder();
                for (int i = 0; i < KeyData.Count; i++)
                    sb.AppendFormat("{0:X2}", KeyData.Array[i + KeyData.Offset]);
                return sb.ToString();
            }
        }
    }

    /// <summary>
    /// Represents a persistent dictionary keyed by ASCII strings. The values are not stored in any given order on disk, and the values are not required to be resident in memory.
    /// At any given time a portion of the Tangle's values may be resident in memory. If a value is not resident in memory, it will be fetched asynchronously from disk.
    /// The Tangle's keys are implicitly ordered, which allows for efficient lookups of individual values by key, or for cheaply locating a range of values with contiguous keys.
    /// Converting values to/from their disk format is handled by the provided TangleSerializer and TangleDeserializer.
    /// The Tangle's disk storage engine partitions its storage up into pages based on the provided page size. For optimal performance, this should be an integer multiple of the size of a memory page (typically 4KB).
    /// </summary>
    /// <typeparam name="T">The type of the value stored within the tangle.</typeparam>
    public unsafe class Tangle<T> : IDisposable {
        public static TangleSerializer<T> DefaultSerializer;
        public static TangleDeserializer<T> DefaultDeserializer;

        static Tangle () {
            DefaultDeserializer = (Stream i, out T o) => {
                var ser = new XmlSerializer(typeof(T));
                var temp = ser.Deserialize(i);
                o = (T)temp;
            };
            DefaultSerializer = (ref T i, Stream o) => {
                var ser = new XmlSerializer(typeof(T));
                ser.Serialize(o, i);
            };
        }

        public const string ExplanatoryPlaceholderText =
@"This is a DataMangler database. 
All the actual data is stored in NTFS streams attached to this file.
For more information, see http://support.microsoft.com/kb/105763.";

        public const uint CurrentFormatVersion = 1;

        public readonly string Filename;
        public readonly TaskScheduler Scheduler;
        public readonly TangleSerializer<T> Serializer;
        public readonly TangleDeserializer<T> Deserializer;

        protected readonly ReaderWriterLockSlim IndexLock = new ReaderWriterLockSlim(LockRecursionPolicy.NoRecursion);

        private readonly StreamRef IndexStream;
        private readonly StreamRef KeyStream;
        private readonly StreamRef DataStream;

        public Tangle (
            TaskScheduler scheduler, string filename, 
            TangleSerializer<T> serializer = null, 
            TangleDeserializer<T> deserializer = null
        ) {
            Scheduler = scheduler;
            Filename = filename;
            Serializer = serializer ?? DefaultSerializer;
            Deserializer = deserializer ?? DefaultDeserializer;

            File.WriteAllText(Filename, ExplanatoryPlaceholderText);

            IndexStream = new StreamRef(Filename, "index");
            KeyStream = new StreamRef(Filename, "keys");
            DataStream = new StreamRef(Filename, "data");

            VersionCheck(IndexStream);
            VersionCheck(KeyStream);
            VersionCheck(DataStream);
        }

        private static void VersionCheck (StreamRef stream) {
            if (stream.FormatVersion < CurrentFormatVersion) {
                Console.WriteLine("Format upgrade not implemented");
                stream.FormatVersion = CurrentFormatVersion;
            }
        }

        private static long StreamAppend<U> (StreamRef stream, ref U value) 
            where U : struct {
            uint entrySize = (uint)Marshal.SizeOf(typeof(U));
            long spot = stream.AllocateSpace(entrySize);
            using (var range = stream.AccessRange(spot, entrySize, MemoryMappedFileAccess.Write))
                range.View.Write<U>(0, ref value);
            return spot;
        }

        private static long StreamAppend (StreamRef stream, ArraySegment<byte> data) {
            uint entrySize = (uint)data.Count;
            long spot = stream.AllocateSpace(entrySize);
            using (var range = stream.AccessRange(spot, entrySize, MemoryMappedFileAccess.Write))
                range.View.WriteArray(0, data.Array, data.Offset, data.Count);
            return spot;
        }

        /// <summary>
        /// Reads a value from the tangle, looking it up via its key.
        /// </summary>
        /// <returns>A future that will contain the value once it has been read.</returns>
        public Future<T> Get (TangleKey key) {
            return Future.RunInThread(() => {
                T result;
                if (InternalGet(key, out result))
                    return result;
                else
                    throw new KeyNotFoundException(key.ToString());
            });
        }

        /// <summary>
        /// Reads a range of values from the tangle, using a pair of keys to specify the range.
        /// </summary>
        /// <param name="first">The key of the first value to retrieve.</param>
        /// <param name="last">The key of the last value to retrieve.</param>
        /// <returns>All the values from the tangle, ordered sequentially by key.</returns>
        public Future<T[]> Get (TangleKey first, TangleKey last) {
            var f = new Future<T[]>();
            f.SetResult(null, new NotImplementedException());
            return f;
        }

        /// <summary>
        /// Stores a value into the tangle, assigning it a given key.
        /// </summary>
        /// <returns>A future that completes once the value has been stored to disk.</returns>
        public IFuture Set (TangleKey key, T value) {
            return Future.RunInThread(
                () => InternalSet(key, value)
            );
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

        private bool IndexEntryByKey (ref TangleKey key, out IndexEntry result) {
            uint entrySize = (uint)Marshal.SizeOf(typeof(IndexEntry));
            uint lhsLength = (uint)key.KeyData.Count;
            var length = IndexStream.Length;

            fixed (byte * pLhs = &key.KeyData.Array[key.KeyData.Offset])
            for (long position = 0; (position + entrySize) <= length; position += entrySize) {

                using (var indexRange = IndexStream.AccessRange(position, entrySize, MemoryMappedFileAccess.Read))
                using (var rawPtr = indexRange.GetPointer()) {
                    var pEntry = (IndexEntry *)rawPtr.Pointer;

                    if (pEntry->IsValid != 1)
                        continue;

                    using (var keyRange = KeyStream.AccessRange(
                        pEntry->KeyOffset, pEntry->KeyLength, MemoryMappedFileAccess.Read
                    ))
                    using (var rhs = keyRange.GetPointer()) {

                        if (CompareKeys(
                                pLhs, lhsLength, rhs.Pointer, pEntry->KeyLength
                            ) == 0
                        ) {
                            result = *pEntry;
                            return true;
                        }
                    }

                }

            }

            result = default(IndexEntry);
            return false;
        }

        private void WriteNewPair (TangleKey key, ref T value, out IndexEntry indexEntry) {
            long dataPos;
            uint dataLen;
            using (var ms = new MemoryStream()) {
                Serializer(ref value, ms);
                dataLen = (uint)ms.Length;
                dataPos = StreamAppend(
                    DataStream, new ArraySegment<byte>(
                        ms.GetBuffer(), 0, (int)ms.Length
                    )
                );
            }

            var keyPos = StreamAppend(KeyStream, key.KeyData);

            indexEntry = new IndexEntry {
                DataOffset = dataPos,
                KeyOffset = keyPos,
                DataLength = dataLen,
                KeyLength = (uint)key.KeyData.Count,
                IsValid = 0
            };
        }

        private void ReadValue (ref IndexEntry entry, out T value) {
            byte[] buffer = new byte[entry.DataLength];
            using (var range = DataStream.AccessRange(entry.DataOffset, entry.DataLength))
                range.View.ReadArray(0, buffer, 0, buffer.Length);

            using (var ms = new MemoryStream(buffer, false))
                Deserializer(ms, out value);
        }

        private void InternalSet (TangleKey key, T value) {
            uint entrySize = (uint)Marshal.SizeOf(typeof(IndexEntry));

            IndexEntry indexEntry;
            WriteNewPair(key, ref value, out indexEntry);

            long offset = IndexStream.AllocateSpace(entrySize);
            using (var indexRange = IndexStream.AccessRange(offset, entrySize, MemoryMappedFileAccess.ReadWrite))
            using (var rawPtr = indexRange.GetPointer()) {
                var pEntry = (IndexEntry*)rawPtr.Pointer;
                *pEntry = indexEntry;

                var wasValid = Interlocked.CompareExchange(ref pEntry->IsValid, 1, 0);
                if (wasValid != 0)
                    throw new InvalidDataException("Index corrupted");
            }
        }

        private bool InternalGet (TangleKey key, out T value) {
            IndexEntry indexEntry;
            if (!IndexEntryByKey(ref key, out indexEntry)) {
                value = default(T);
                return false;
            }

            ReadValue(ref indexEntry, out value);
            return true;
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
                    access.View.ReadArray(0, buffer, 0, size);
                fs.Write(buffer, 0, size);
            }
        }

        public IEnumerator<object> Dispose () {
            IndexStream.Dispose();
            DataStream.Dispose();
            KeyStream.Dispose();

            yield break;
        }

        void IDisposable.Dispose () {
            Scheduler.WaitFor(Dispose());
        }
    }
}