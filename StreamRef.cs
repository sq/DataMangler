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
using System.Diagnostics;
using System.IO.MemoryMappedFiles;
using System.Threading;
using System.Runtime.InteropServices;
using System.IO;
using Microsoft.Win32.SafeHandles;
using System.Collections.Generic;
using System.Security;
using System.Collections.Concurrent;
using Squared.Util;

namespace Squared.Data.Mangler.Internal {
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    internal unsafe struct StreamHeader {
        public uint FormatVersion;
        public long DataLength;
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
        private readonly MemoryMappedViewAccessor View;
        private readonly ViewCache.CacheEntry CacheEntry;

        public StreamRange (StreamRef stream, MemoryMappedViewAccessor view, long offset, uint size, long actualOffset, long actualSize) {
            Stream = stream;
            View = view;
            CacheEntry = default(ViewCache.CacheEntry);
            Offset = offset;
            Size = size;
            Buffer = view.GetSafeBuffer();
            Pointer = null;
            Buffer.AcquirePointer(ref Pointer);
            Pointer += view.GetPointerOffset();
            Pointer += (offset - actualOffset);
        }

        public StreamRange (StreamRef stream, ViewCache.CacheEntry cacheEntry, long offset, uint size) {
            Stream = stream;
            CacheEntry = cacheEntry;
            View = null;
            Offset = offset;
            Size = size;
            Buffer = cacheEntry.Buffer;
            Pointer = cacheEntry.Pointer + cacheEntry.PointerOffset;
            Pointer += (offset - cacheEntry.Offset);
        }

        public void Dispose () {
            if (View != null) {
                Buffer.ReleasePointer();
                View.Dispose();
            } else
                CacheEntry.RemoveRef();
        }
    }

    [Flags]
    public enum NativeFileAccess : uint {
        GenericRead = 0x80000000,
        GenericWrite = 0x40000000
    }

    [Flags]
    public enum NativeFileFlags : uint {
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

    public static class Native {
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
        public unsafe class CacheEntry : IDisposable {
            public readonly long CreatedWhen;
            public readonly SafeBuffer Buffer;
            public readonly byte* Pointer;
            public readonly long PointerOffset;
            public readonly long Offset;
            public readonly uint Size;
            public readonly MemoryMappedViewAccessor View;

            internal bool IsDisposed;
            private int _RefCount;

            public CacheEntry (MemoryMappedViewAccessor view, long offset, uint size) {
                CreatedWhen = Time.Ticks;
                View = view;
                Offset = offset;
                Size = size;
                IsDisposed = false;
                _RefCount = 1;
                Buffer = view.GetSafeBuffer();
                Pointer = null;
                Buffer.AcquirePointer(ref Pointer);
                PointerOffset = view.GetPointerOffset();
            }

            public void AddRef () {
                if (IsDisposed)
                    throw new ObjectDisposedException("CacheEntry");

                Interlocked.Increment(ref _RefCount);
            }

            public void RemoveRef () {
                if (IsDisposed) {
                    // This can happen if the stream is grown while a reference is held to one of its views.
                    // In this case, we don't want using() blocks and finally handlers to throw exceptions.

                    if (Interlocked.Decrement(ref _RefCount) < 0)
                        throw new ObjectDisposedException("CacheEntry");

                    return;
                }

                if (Interlocked.Decrement(ref _RefCount) == 0)
                    Release();
            }

            void IDisposable.Dispose () {
                RemoveRef();
            }

            internal void Release () {
                IsDisposed = true;
                Buffer.ReleasePointer();
                View.Dispose();
            }
        }

        public readonly MemoryMappedFile File;
        public readonly long FileLength;
        public readonly int Capacity;
        private readonly CacheEntry[] Cache;

        public ViewCache (MemoryMappedFile file, long fileLength, int capacity = 4) {
            File = file;
            FileLength = fileLength;
            Capacity = capacity;
            Cache = new CacheEntry[capacity];
        }

        public MemoryMappedViewAccessor CreateViewUncached (long offset, uint size, MemoryMappedFileAccess access, out long actualOffset, out uint actualSize) {
            unchecked {
                const uint chunkSize = 1024 * 1024 * 16;

                actualOffset = (offset / chunkSize * chunkSize);
                if (actualOffset < 0)
                    actualOffset = 0;

                var actualEnd = ((offset + size) + (chunkSize - 1)) / chunkSize * chunkSize;
                if (actualEnd < actualOffset)
                    actualEnd = actualOffset;
                if (actualEnd >= FileLength)
                    actualEnd = FileLength;

                actualSize = (uint)(actualEnd - actualOffset);

                return File.CreateViewAccessor(actualOffset, actualSize, access);
            }
        }

        public CacheEntry CreateView (long offset, uint size, MemoryMappedFileAccess access) {
            if (access == MemoryMappedFileAccess.Write)
                access = MemoryMappedFileAccess.ReadWrite;

            int? freeSlot = null;
            int? oldestUsedSlot = null;
            long oldestUsedTimestamp = long.MaxValue;

            for (int i = 0; i < Capacity; i++) {
                var item = Cache[i];

                if (item == null || item.IsDisposed) {
                    freeSlot = i;
                    continue;
                }

                if (item.CreatedWhen < oldestUsedTimestamp)
                    oldestUsedSlot = i;

                if (offset < item.Offset)
                    continue;
                if (offset + size > item.Offset + item.Size)
                    continue;

                item.AddRef();
                return item;
            }

            if (!freeSlot.HasValue && !oldestUsedSlot.HasValue)
                throw new InvalidDataException();

            long actualOffset;
            uint actualSize;
            var view = CreateViewUncached(offset, size, MemoryMappedFileAccess.ReadWrite, out actualOffset, out actualSize);

            var newEntry = new CacheEntry(view, actualOffset, actualSize);
            newEntry.AddRef();

            long slotIndex = freeSlot.GetValueOrDefault(oldestUsedSlot.GetValueOrDefault(0));
            var oldEntry = Interlocked.Exchange(ref Cache[slotIndex], newEntry);

            if (oldEntry != null)
                oldEntry.RemoveRef();

            return newEntry;
        }

        public void Dispose () {
            for (int i = 0; i < Capacity; i++) {
                var ce = Interlocked.Exchange(ref Cache[i], null);

                if (ce != null)
                    ce.Release();
            }
        }
    }

    internal class StreamRef : IDisposable {
        public static readonly uint HeaderSize = (uint)Marshal.SizeOf(typeof(StreamHeader));
        public const uint InitialSize = 256 * 1024;
        public const uint DoublingThreshold = 1024 * 1024 * 64;
        public const uint PostDoublingGrowthRate = 1024 * 1024 * 8;

        public event EventHandler LengthChanging, LengthChanged;

        protected ViewCache Cache;
        protected MemoryMappedFile Handle;
        protected MemoryMappedViewAccessor HeaderView;

        public readonly FileStream NativeStream;
        public readonly bool OwnsStream;

        protected long StreamCapacity;

        public StreamRef (FileStream nativeStream, bool ownsStream = true) {
            NativeStream = nativeStream;
            OwnsStream = ownsStream;

            CreateHandles(InitialSize);
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
            long newCapacity = StreamCapacity;
            if (StreamCapacity >= DoublingThreshold) {
                while (newCapacity < capacity)
                    newCapacity += PostDoublingGrowthRate;
            } else {
                while (newCapacity < capacity)
                    newCapacity *= 2;
            }

            if (LengthChanging != null)
                LengthChanging(this, EventArgs.Empty);

            DisposeViews();

            CreateHandles(newCapacity);

            if (LengthChanged != null)
                LengthChanged(this, EventArgs.Empty);
        }

        /// <summary>
        /// Allocates <paramref name="size"/> byte(s) of unused space at the end of the stream.
        /// </summary>
        /// <param name="size">The number of bytes to allocate.</param>
        /// <returns>The offset into the stream where the allocated bytes are located.</returns>
        public unsafe long? AllocateSpace (uint size) {
            if (size == 0)
                return null;

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
            }

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
            unchecked {
                long actualBegin = offset + HeaderSize;
                uint actualSize = size;

                EnsureCapacity(HeaderSize + offset + actualSize);

                var viewRef = Cache.CreateView(actualBegin, actualSize, access);

                return new StreamRange(
                    this, viewRef, actualBegin, actualSize
                );
            }
        }

        /// <summary>
        /// Accesses a range of bytes within the stream, bypassing the stream cache.
        /// </summary>
        /// <param name="offset">The offset within the stream, relative to the end of the stream header.</param>
        /// <param name="size">The size of the range to access, in bytes.</param>
        public StreamRange AccessRangeUncached (long offset, uint size, MemoryMappedFileAccess access = MemoryMappedFileAccess.ReadWrite) {
            unchecked {
                long relativeOffset = offset + HeaderSize;

                EnsureCapacity(relativeOffset + size);

                long actualOffset;
                uint actualSize;
                var view = Cache.CreateViewUncached(relativeOffset, size, access, out actualOffset, out actualSize);

                return new StreamRange(
                    this, view, relativeOffset, size, actualOffset, actualSize
                );
            }
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

            if (LengthChanging != null)
                LengthChanging(this, EventArgs.Empty);

            DisposeViews();

            NativeStream.SetLength(totalLength);
            if (OwnsStream)
                NativeStream.Dispose();
        }
    }
}