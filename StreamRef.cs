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

namespace Squared.Data.Mangler.Internal {
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    internal unsafe struct StreamHeader {
        [MarshalAs(UnmanagedType.I4)]
        public uint FormatVersion;
        [MarshalAs(UnmanagedType.I8)]
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

    internal unsafe class StreamRange : IDisposable {
        public readonly StreamRef Stream;

        private readonly byte* Pointer;
        private readonly long PointerOffset;

        public readonly long Offset, Size;

        private readonly SafeBuffer Buffer;
        private readonly MemoryMappedViewAccessor View;

        public StreamRange (StreamRef stream, MemoryMappedViewAccessor view, long offset, uint size) {
            Stream = stream;
            View = view;
            Offset = offset;
            Size = size;
            PointerOffset = view.GetPointerOffset();
            Buffer = view.GetSafeBuffer();
            Buffer.AcquirePointer(ref Pointer);
        }

        public byte* GetPointer () {
            return Pointer + PointerOffset;
        }

        public void Dispose () {
            Buffer.ReleasePointer();
            View.Dispose();
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

        [DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Auto)]
        public static extern bool DeviceIoControl (
            SafeFileHandle handle, uint dwIoControlCode,
            IntPtr lpInBuffer, uint nInBufferSize,
            IntPtr lpOutBuffer, uint nOutBufferSize,
            out uint lpBytesReturned, IntPtr lpOverlapped
        );

        public const uint FSCTL_SET_SPARSE = 0x900C4;
    }

    internal class StreamRef : IDisposable {
        public static readonly uint HeaderSize = (uint)Marshal.SizeOf(typeof(StreamHeader));
        public const uint InitialCapacity = 32 * 1024;
        public const uint GrowthRate = 64 * 1024;

        public readonly string Filename;
        public readonly string StreamName;

        protected MemoryMappedFile Handle;
        protected MemoryMappedViewAccessor HeaderView;
        protected FileStream Stream;

        protected long StreamCapacity;

        public StreamRef (string filename, string streamName) {
            Filename = filename;
            StreamName = streamName;

            CreateHandles(InitialCapacity);
        }

        protected FileStream OpenAlternateStream (string filename, string streamName) {
            const string prefix = @"\\?\";
            var path = String.Format("{0}{1}:{2}", prefix, filename, streamName);
            var handle = Native.CreateFile(
                path,
                NativeFileAccess.GenericRead | NativeFileAccess.GenericWrite,
                FileShare.None, IntPtr.Zero, FileMode.OpenOrCreate,
                NativeFileFlags.RandomAccess, IntPtr.Zero
            );
            if (handle.IsInvalid || handle.IsClosed)
                throw new IOException("Could not open stream " + path);
            return new FileStream(handle, FileAccess.ReadWrite);
        }

        protected void CreateHandles (long capacity) {
            Stream = OpenAlternateStream(Filename, StreamName);
            if (Stream.Length > capacity)
                capacity = Stream.Length;

            Handle = MemoryMappedFile.CreateFromFile(
                Stream, null, capacity,
                MemoryMappedFileAccess.ReadWrite,
                null, HandleInheritability.None, false
            );
            HeaderView = Handle.CreateViewAccessor(0, HeaderSize);
            StreamCapacity = capacity;
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
            var newCapacity = (capacity + GrowthRate - 1) / GrowthRate * GrowthRate;

            Dispose();

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

        public unsafe long Length {
            get {
                using (var header = AccessHeader())
                    return header.Ptr->DataLength;
            }
        }

        public long Capacity {
            get {
                return Math.Max(StreamCapacity, Stream.Length);
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

            return new StreamRange(
                this, Handle.CreateViewAccessor(
                    actualBegin, actualSize, MemoryMappedFileAccess.ReadWrite
                ), actualBegin, actualSize
            );
        }

        public void Dispose () {
            HeaderView.Flush();
            HeaderView.Dispose();
            Handle.Dispose();
        }
    }
}