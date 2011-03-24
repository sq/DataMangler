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
using System.IO.MemoryMappedFiles;
using System.Threading;
using System.Runtime.InteropServices;
using System.IO;
using Microsoft.Win32.SafeHandles;
using System.Collections.Generic;
using System.Security;

namespace Squared.Data.Mangler.Internal {
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    internal unsafe struct StreamHeader {
        public uint FormatVersion;
        public long DataLength;
        public long RootIndex;
    }

    internal unsafe struct StreamHeaderRef : IDisposable {
        public readonly StreamHeader* Ptr;

        private readonly MemoryMappedViewAccessor Accessor;
        private readonly SafeBuffer Buffer;

        public StreamHeaderRef (MemoryMappedViewAccessor accessor) {
            Accessor = accessor;
            Buffer = accessor.GetSafeBuffer();

            byte* temp = null;
            Buffer.AcquirePointer(ref temp);

            Ptr = (StreamHeader*)temp;
        }

        public void Dispose () {
            Buffer.ReleasePointer();
        }
    }

    internal unsafe struct StreamRange : IDisposable {
        public readonly StreamRef Stream;

        public readonly byte* Pointer;

        public readonly long Offset, Size;

        private readonly SafeBuffer Buffer;
        private readonly ViewCache.Ref ViewRef;

        public StreamRange (StreamRef stream, ViewCache.Ref viewRef, long offset, uint size) {
            Stream = stream;
            ViewRef = viewRef;
            Offset = offset;
            Size = size;
            Buffer = viewRef.View.GetSafeBuffer();
            Pointer = null;
            Buffer.AcquirePointer(ref Pointer);
            Pointer += viewRef.View.GetPointerOffset();
            Pointer += (offset - viewRef.Offset);
        }

        public void Dispose () {
            Buffer.ReleasePointer();
            ViewRef.Dispose();
        }
    }

    [Flags]
    internal enum NativeFileAccess : uint {
        GenericRead = 0x80000000,
        GenericWrite = 0x40000000
    }

    [Flags]
    internal enum NativeFileFlags : uint {
        WriteThrough = 0x80000000,
        Overlapped = 0x40000000,
        NoBuffering = 0x20000000,
        RandomAccess = 0x10000000,
        SequentialScan = 0x8000000,
        DeleteOnClose = 0x4000000,
        BackupSemantics = 0x2000000,
        PosixSemantics = 0x1000000,
        OpenReparsePoint = 0x200000,
        OpenNoRecall = 0x100000
    }

    internal static class Native {
        [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
        public static extern SafeFileHandle CreateFile (
            string filename,
            NativeFileAccess access,
            FileShare share,
            IntPtr security,
            FileMode mode,
            NativeFileFlags flags,
            IntPtr template
        );
        
        [DllImport("msvcrt.dll", CallingConvention = CallingConvention.Cdecl)]
        [SuppressUnmanagedCodeSecurity]
        public static extern unsafe int memcmp (byte * lhs, byte * rhs, UIntPtr count);

        [DllImport("msvcrt.dll", CallingConvention = CallingConvention.Cdecl)]
        [SuppressUnmanagedCodeSecurity]
        public static extern unsafe int memmove (byte* dest, byte* src, UIntPtr count);

        [DllImport("msvcrt.dll", CallingConvention = CallingConvention.Cdecl)]
        [SuppressUnmanagedCodeSecurity]
        public static extern unsafe int memset (byte* dest, int value, UIntPtr count);

        public static FileStream OpenAlternateStream (string filename, string streamName) {
            const string prefix = @"\\?\";
            var path = String.Format("{0}{1}:{2}", prefix, filename, streamName);
            var handle = Native.CreateFile(
                path,
                NativeFileAccess.GenericRead | NativeFileAccess.GenericWrite,
                FileShare.Delete | FileShare.ReadWrite, IntPtr.Zero, FileMode.OpenOrCreate,
                NativeFileFlags.RandomAccess, IntPtr.Zero
            );
            if (handle.IsInvalid || handle.IsClosed)
                throw new IOException("Could not open stream " + path);
            return new FileStream(handle, FileAccess.ReadWrite);
        }
    }

    internal class ViewCache : IDisposable {
        public struct Ref : IDisposable {
            public readonly MemoryMappedViewAccessor View;
            private readonly CacheEntry Entry;
            
            public Ref (CacheEntry entry) {
                Entry = entry;
                View = Entry.View;
                Entry.AddRef();
            }

            public long Offset {
                get {
                    return Entry.Offset;
                }
            }

            public uint Size {
                get {
                    return Entry.Size;
                }
            }

            public void Dispose () {
                Entry.RemoveRef();
            }
        }

        public class CacheEntry : IDisposable {
            public readonly long Offset;
            public readonly uint Size;
            public readonly MemoryMappedViewAccessor View;
            public int RefCount;

            public CacheEntry (MemoryMappedViewAccessor view, long offset, uint size) {
                View = view;
                Offset = offset;
                Size = size;
                RefCount = 1;
            }

            public void AddRef () {
                RefCount += 1;
            }

            public void RemoveRef () {
                RefCount -= 1;
                if (RefCount <= 0)
                    Dispose();
            }

            public void Dispose () {
                View.Dispose();
            }
        }

        public readonly MemoryMappedFile File;
        public readonly long FileLength;
        public readonly int Capacity;
        private readonly Queue<CacheEntry> ReadCache, ReadWriteCache;

        public ViewCache (MemoryMappedFile file, long fileLength, int capacity = 3) {
            File = file;
            FileLength = fileLength;
            Capacity = capacity;
            ReadCache = new Queue<CacheEntry>(capacity);
            ReadWriteCache = new Queue<CacheEntry>(capacity);
        }

        public Ref CreateView (long offset, uint size, MemoryMappedFileAccess access) {
            if (access == MemoryMappedFileAccess.Write)
                access = MemoryMappedFileAccess.ReadWrite;

            Queue<CacheEntry> cache;
            if (access == MemoryMappedFileAccess.Read)
                cache = ReadCache;
            else if (access == MemoryMappedFileAccess.ReadWrite)
                cache = ReadWriteCache;
            else
                throw new ArgumentException("Invalid access mode: Only Read, ReadWrite and Write are acceptable");

            foreach (var item in cache) {
                if (offset < item.Offset)
                    continue;
                if (offset + size > item.Offset + item.Size)
                    continue;

                return new Ref(item);
            }

            if (cache.Count > Capacity) {
                CacheEntry ce = cache.Dequeue();
                ce.RemoveRef();
            }

            const uint chunkSize = 1024 * 1024 * 8;

            var actualOffset = (offset / chunkSize * chunkSize);
            if (actualOffset < 0)
                actualOffset = 0;

            var actualEnd = ((offset + size) + (chunkSize - 1)) / chunkSize * chunkSize;
            if (actualEnd < actualOffset)
                actualEnd = actualOffset;
            if (actualEnd >= FileLength)
                actualEnd = FileLength;

            var actualSize = (uint)(actualEnd - actualOffset);

            var view = File.CreateViewAccessor(actualOffset, actualSize, access);

            var newEntry = new CacheEntry(view, actualOffset, actualSize);
            cache.Enqueue(newEntry);
            return new Ref(newEntry);
        }

        public void Dispose () {
            while (ReadCache.Count > 0)
                ReadCache.Dequeue().Dispose();
            while (ReadWriteCache.Count > 0)
                ReadWriteCache.Dequeue().Dispose();
        }
    }

    internal class StreamRef : IDisposable {
        public static readonly uint HeaderSize = (uint)Marshal.SizeOf(typeof(StreamHeader));
        public const uint ChunkSize = 256 * 1024;

        protected ViewCache Cache;
        protected MemoryMappedFile Handle;
        protected MemoryMappedViewAccessor HeaderView;

        public readonly FileStream NativeStream;
        public readonly bool OwnsStream;

        protected long StreamCapacity;

        public StreamRef (FileStream nativeStream, bool ownsStream = true) {
            NativeStream = nativeStream;
            OwnsStream = ownsStream;

            CreateHandles(ChunkSize);
        }

        protected void CreateHandles (long capacity) {
            if (NativeStream.Length > capacity)
                capacity = NativeStream.Length;

            Handle = MemoryMappedFile.CreateFromFile(
                NativeStream, null, capacity,
                MemoryMappedFileAccess.ReadWrite,
                null, HandleInheritability.None, true
            );
            HeaderView = Handle.CreateViewAccessor(0, HeaderSize);
            StreamCapacity = capacity;
            Cache = new ViewCache(Handle, StreamCapacity);
        }

        internal unsafe StreamHeaderRef AccessHeader () {
            StreamHeaderRef result;

            result = new StreamHeaderRef(HeaderView);

            return result;
        }

        protected void EnsureCapacity (long capacity) {
            if (capacity <= StreamCapacity)
                return;

            // We grow the stream by a fixed amount every time we run out
            //  of space. Doubling or some other algorithm might be better,
            //  but this is simple and predictable.
            var newCapacity = (capacity + ChunkSize - 1) / ChunkSize * ChunkSize;

            DisposeViews();

            CreateHandles(newCapacity);
        }

        /// <summary>
        /// Allocates <paramref name="size"/> byte(s) of unused space at the end of the stream.
        /// </summary>
        /// <param name="size">The number of bytes to allocate.</param>
        /// <returns>The offset into the stream where the allocated bytes are located.</returns>
        public unsafe long AllocateSpace (uint size) {
            long oldSize, newSize;

            // This is thread-safe, but because we bump the DataLength without
            //  making any effort to ensure the data in the region is valid,
            //  other threads may attempt to read it and find random garbage
            //  there.
            // On the bright side, MSDN claims that unused regions in a mapped
            //  file are always zeroes, and this seems to be true so far. Given
            //  this, most of the time you just need a 'this data is valid' bit
            //  tucked away to protect yourself from reading uninitialized data.
            using (var header = AccessHeader()) {
                newSize = Interlocked.Add(ref header.Ptr->DataLength, size);
                oldSize = newSize - size;
            };

            EnsureCapacity(newSize + HeaderSize);
            return oldSize;
        }

        public unsafe uint FormatVersion {
            get {
                using (var header = AccessHeader())
                    return header.Ptr->FormatVersion;
            }
            set {
                using (var header = AccessHeader())
                    header.Ptr->FormatVersion = value;
            }
        }

        public unsafe long RootIndex {
            get {
                using (var header = AccessHeader())
                    return header.Ptr->RootIndex;
            }
        }

        public unsafe bool MoveRoot (long oldIndex, long newIndex) {
            using (var header = AccessHeader())
                return Interlocked.CompareExchange(ref header.Ptr->RootIndex, newIndex, oldIndex) == oldIndex;
        }

        public unsafe long Length {
            get {
                using (var header = AccessHeader())
                    return header.Ptr->DataLength;
            }
        }

        public long Capacity {
            get {
                return Math.Max(StreamCapacity, NativeStream.Length);
            }
        }

        /// <summary>
        /// Accesses a range of bytes within the stream.
        /// </summary>
        /// <param name="offset">The offset within the stream, relative to the end of the stream header.</param>
        /// <param name="size">The size of the range to access, in bytes.</param>
        public StreamRange AccessRange (long offset, uint size, MemoryMappedFileAccess access = MemoryMappedFileAccess.ReadWrite) {
            long actualBegin = offset + HeaderSize;
            uint actualSize = size;

            EnsureCapacity(HeaderSize + offset + actualSize);

            var viewRef = Cache.CreateView(actualBegin, actualSize, access);

            return new StreamRange(
                this, viewRef, actualBegin, actualSize
            );
        }

        private void DisposeViews () {
            if (Cache != null) {
                Cache.Dispose();
                Cache = null;
            }
            if (HeaderView != null) {
                HeaderView.Flush();
                HeaderView.Dispose();
                HeaderView = null;
            }
            if (Handle != null) {
                Handle.Dispose();
                Handle = null;
            }
        }

        protected unsafe long GetTotalLength () {
            using (var header = AccessHeader())
                return header.Ptr->DataLength + HeaderSize;
        }

        public void Dispose () {
            long totalLength = GetTotalLength();

            DisposeViews();

            NativeStream.SetLength(totalLength);
            if (OwnsStream)
                NativeStream.Dispose();
        }
    }
}