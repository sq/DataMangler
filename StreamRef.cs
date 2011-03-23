using System;
using System.IO.MemoryMappedFiles;
using System.Threading;
using System.Runtime.InteropServices;
using System.IO;
using Microsoft.Win32.SafeHandles;
using System.Reflection;

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
        private readonly Action Cleanup;

        public StreamHeaderRef (MemoryMappedViewAccessor accessor, Action cleanup) {
            Accessor = accessor;
            Cleanup = cleanup;
            Buffer = accessor.GetSafeBuffer();

            byte* temp = null;
            Buffer.AcquirePointer(ref temp);

            Ptr = (StreamHeader*)temp;
        }

        public void Dispose () {
            Accessor.Flush();
            Buffer.ReleasePointer();
            Cleanup();
        }
    }

    internal class StreamRange : IDisposable {
        public readonly StreamRef Stream;
        public readonly MemoryMappedViewAccessor View;
        public readonly long Offset;
        public readonly uint Size;

        public StreamRange (StreamRef stream, MemoryMappedViewAccessor view, long offset, uint size) {
            Stream = stream;
            View = view;
            Offset = offset;
            Size = size;
        }

        public AcquiredPointer GetPointer () {
            var result = View.GetPointer(Offset);
            return result;
        }

        public void Dispose () {
            View.Flush();
            View.Dispose();
            Stream.OnRangeReleased();
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

        // Used for globally locking the state of the stream.
        //  A write lock is acquired any time we intend to manipulate the Handle or HeaderView fields.
        //  A read lock is acquired any time we are using the contents of the mapped file.
        public readonly ReaderWriterLockSlim Lock;

        protected int OutstandingHeaderLocks, OutstandingRangeLocks;
        protected long StreamCapacity;

        public StreamRef (string filename, string streamName) {
            Lock = new ReaderWriterLockSlim(LockRecursionPolicy.NoRecursion);
            Filename = filename;
            StreamName = streamName;

            Lock.EnterWriteLock();
            try {
                CreateHandles(InitialCapacity);
            } finally {
                Lock.ExitWriteLock();
            }
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

        // A write lock must be held.
        protected void CreateHandles (long capacity) {
            if (!Lock.IsWriteLockHeld)
                throw new SynchronizationLockException("Cannot create handles without holding the write lock.");

            Stream = OpenAlternateStream(Filename, StreamName);
            if (Stream.Length > capacity)
                capacity = Stream.Length;

            Handle = MemoryMappedFile.CreateFromFile(
                Stream, null, capacity, 
                MemoryMappedFileAccess.ReadWrite,
                null, HandleInheritability.None, false
            );
            HeaderView = Handle.CreateViewAccessor(0, Marshal.SizeOf(typeof(StreamHeader)));
            StreamCapacity = capacity;
        }

        internal void OnRangeReleased () {
            Interlocked.Decrement(ref OutstandingRangeLocks);
            Lock.ExitReadLock();
        }

        protected void OnHeaderReleased () {
            Interlocked.Decrement(ref OutstandingHeaderLocks);
            Lock.ExitReadLock();
        }

        internal unsafe StreamHeaderRef AccessHeader () {
            StreamHeaderRef result;

            Lock.EnterReadLock();
            try {
                result = new StreamHeaderRef(HeaderView, OnHeaderReleased);
            } catch {
                Lock.ExitReadLock();
                throw;
            }

            Interlocked.Increment(ref OutstandingHeaderLocks);
            return result;
        }

        protected void EnsureCapacity (long capacity) {
            Lock.EnterUpgradeableReadLock();
            try {
                if (capacity <= StreamCapacity)
                    return;

                Lock.EnterWriteLock();

                try {
                    var newCapacity = (capacity + GrowthRate - 1) / GrowthRate * GrowthRate;

                    // This should never happen.
                    if (OutstandingHeaderLocks > 0)
                        throw new InvalidDataException("Header is locked, so the stream cannot be expanded.");

                    HeaderView.Flush();
                    HeaderView.Dispose();
                    Handle.Dispose();

                    CreateHandles(newCapacity);
                } finally {
                    Lock.ExitWriteLock();
                }
            } finally {
                Lock.ExitUpgradeableReadLock();
            }
        }

        /// <summary>
        /// Allocates <paramref name="size"/> byte(s) of unused space at the end of the stream.
        /// </summary>
        /// <param name="size">The number of bytes to allocate.</param>
        /// <returns>The offset into the stream where the allocated bytes are located.</returns>
        public unsafe long AllocateSpace (uint size) {
            long oldSize, newSize;

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
            switch (access) {
                case MemoryMappedFileAccess.Write:
                case MemoryMappedFileAccess.ReadWrite:
                case MemoryMappedFileAccess.ReadWriteExecute:
                case MemoryMappedFileAccess.CopyOnWrite:
                    EnsureCapacity(HeaderSize + offset + size);
                    break;
            }

            StreamRange result;

            Lock.EnterReadLock();
            try {
                result = new StreamRange(
                    this, Handle.CreateViewAccessor(
                        offset + HeaderSize, size, access
                    ), offset + HeaderSize, size
                );
            } catch {
                Lock.ExitReadLock();
                throw;
            }
            Interlocked.Increment(ref OutstandingRangeLocks);
            return result;
        }

        public void Dispose () {
            Lock.EnterWriteLock();
            try {
                HeaderView.Flush();
                HeaderView.Dispose();
                Handle.Dispose();
            } finally {
                Lock.ExitWriteLock();
                Lock.Dispose();
            }
        }
    }

    public unsafe struct AcquiredPointer : IDisposable {
        public readonly SafeBuffer Buffer;
        public readonly byte* Pointer;

        public AcquiredPointer (UnmanagedMemoryAccessor accessor, long offset = 0) {
            Buffer = accessor.GetSafeBuffer();
            Pointer = null;
            Buffer.AcquirePointer(ref Pointer);
            Pointer += offset;
        }

        public void Dispose () {
            Buffer.ReleasePointer();
        }
    }

    public static class UnmanagedMemoryAccesorExtensions {
        private static readonly FieldInfo BufferField;

        static UnmanagedMemoryAccesorExtensions () {
            BufferField = typeof(UnmanagedMemoryAccessor).GetField(
                "_buffer",
                System.Reflection.BindingFlags.NonPublic |
                System.Reflection.BindingFlags.Instance
            );
        }
        
        internal static SafeBuffer GetSafeBuffer (this UnmanagedMemoryAccessor accessor) {
            var buffer = BufferField.GetValue(accessor) as SafeBuffer;
            if (buffer == null)
                throw new InvalidDataException();
            return buffer;
        }

        internal static AcquiredPointer GetPointer (this UnmanagedMemoryAccessor accessor, long offset) {
            return new AcquiredPointer(accessor, offset);
        }
    }
}