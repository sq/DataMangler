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
using System.Reflection;
using System.Linq.Expressions;

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

    internal class StreamRange : IDisposable {
        public readonly StreamRef Stream;
        public readonly MemoryMappedViewAccessor View;
        public readonly uint Size;

        public StreamRange (StreamRef stream, MemoryMappedViewAccessor view, uint size) {
            Stream = stream;
            View = view;
            Size = size;
        }

        public AcquiredPointer GetPointer () {
            return View.GetPointer();
        }

        public void Dispose () {
            View.Dispose();
        }
    }

    [Flags]
    internal enum NativeFileAccess : uint {
        GenericRead  = 0x80000000,
        GenericWrite = 0x40000000
    }

    [Flags]
    internal enum NativeFileFlags : uint {
        WriteThrough     = 0x80000000,
        Overlapped       = 0x40000000,
        NoBuffering      = 0x20000000,
        RandomAccess     = 0x10000000,
        SequentialScan   = 0x8000000,
        DeleteOnClose    = 0x4000000,
        BackupSemantics  = 0x2000000,
        PosixSemantics   = 0x1000000,
        OpenReparsePoint = 0x200000,
        OpenNoRecall     = 0x100000
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

            HeaderView.Flush();
            HeaderView.Dispose();
            Handle.Dispose();

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
            // Before acquiring a read lock, check to see whether we need to grow the
            //  database first. This check acquires locks of its own.
            EnsureCapacity(HeaderSize + offset + size);

            StreamRange result;

            result = new StreamRange(
                this, Handle.CreateViewAccessor(
                    offset + HeaderSize, size, access
                ), size
            );

            return result;
        }

        public void Dispose () {
            HeaderView.Flush();
            HeaderView.Dispose();
            Handle.Dispose();
        }
    }

    public unsafe struct AcquiredPointer : IDisposable {
        public readonly SafeBuffer Buffer;
        public readonly byte* Pointer;

        public AcquiredPointer (MemoryMappedViewAccessor accessor, long offset = 0) {
            Buffer = accessor.GetSafeBuffer();
            Pointer = null;
            Buffer.AcquirePointer(ref Pointer);
            Pointer += accessor.GetPointerOffset();
        }

        public void Dispose () {
            Buffer.ReleasePointer();
        }
    }

    delegate SafeBuffer GetSafeBufferFunc (UnmanagedMemoryAccessor accessor);
    delegate Int64 GetPointerOffsetFunc (MemoryMappedViewAccessor accessor);

    public static class InternalExtensions {
        private static readonly GetSafeBufferFunc _GetSafeBuffer;
        private static readonly GetPointerOffsetFunc _GetPointerOffset;

        static InternalExtensions () {
            _GetSafeBuffer = CreateGetSafeBuffer();
            _GetPointerOffset = CreateGetPointerOffset();
        }

        // To manipulate structures directly in mapped memory, we have
        //  to be able to get a pointer to the mapping. While this is possible,
        //  the classes for using mapped files do not expose a way to do this
        //  directly. So, we pull out the SafeBuffer object associated with the
        //  mapping and then use a public method to get a pointer.
        // Kind of nasty, but what else can you do?
        private static GetSafeBufferFunc CreateGetSafeBuffer () {
            var t = typeof(UnmanagedMemoryAccessor);
            var field = t.GetField(
                "_buffer",
                System.Reflection.BindingFlags.NonPublic |
                System.Reflection.BindingFlags.Instance
            );
            if (field == null)
                throw new ArgumentNullException();

            var argument = Expression.Parameter(t, "accessor");
            var expr = Expression.Field(argument, field);

            return Expression.Lambda<GetSafeBufferFunc>(
                expr, "GetSafeBuffer", new[] { argument }
            ).Compile();
        }

        // When we get a pointer from a SafeBuffer associated with a mapped
        //  view, the pointer is going to be wrong unless the offset into
        //  the file that we mapped was aligned with a page boundary.
        // So, once we get the pointer, we have to find out the alignment
        //  necessary to line it up with a page, and add that to the pointer
        //  so that we are looking at the start of the mapping, instead of
        //  the start of the page containing the mapping.
        // This is a bit messier than the SafeBuffer hack, because one of the
        //  relevant types - MemoryMappedView - is internal.
        private static GetPointerOffsetFunc CreateGetPointerOffset () {
            var tAccessor = typeof(MemoryMappedViewAccessor);
            var tView = tAccessor.Assembly.GetType(
                "System.IO.MemoryMappedFiles.MemoryMappedView", true
            );

            var fieldView = tAccessor.GetField(
                "m_view",
                System.Reflection.BindingFlags.NonPublic |
                System.Reflection.BindingFlags.Instance
            );
            if (fieldView == null)
                throw new ArgumentNullException();

            var fieldOffset = tView.GetField(
                "m_pointerOffset",
                System.Reflection.BindingFlags.NonPublic |
                System.Reflection.BindingFlags.Instance
            );
            if (fieldOffset == null)
                throw new ArgumentNullException();

            var argument = Expression.Parameter(tAccessor, "accessor");
            var expr = Expression.Field(
                Expression.Field(argument, fieldView), fieldOffset
            );

            return Expression.Lambda<GetPointerOffsetFunc>(
                expr, "GetPointerOffset", new[] { argument }
            ).Compile();
        }
        
        internal static SafeBuffer GetSafeBuffer (this UnmanagedMemoryAccessor accessor) {
            var buffer = _GetSafeBuffer(accessor);
            if (buffer == null)
                throw new InvalidDataException();
            return buffer;
        }

        internal static AcquiredPointer GetPointer (this MemoryMappedViewAccessor accessor) {
            return new AcquiredPointer(accessor);
        }

        internal static Int64 GetPointerOffset (this MemoryMappedViewAccessor accessor) {
            return _GetPointerOffset(accessor);
        }

        internal static ArraySegment<byte> GetSegment (this MemoryStream stream) {
            if (stream.Length >= int.MaxValue)
                throw new InvalidDataException();

            return new ArraySegment<byte>(stream.GetBuffer(), 0, (int)stream.Length);
        }
    }
}