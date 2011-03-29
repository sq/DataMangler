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
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.IO;
using System.Threading;
using System.Xml.Serialization;
using Squared.Data.Mangler.Internal;
using Squared.Data.Mangler.Serialization;

namespace Squared.Data.Mangler {
    [AttributeUsage(AttributeTargets.Method, Inherited = false, AllowMultiple = false)]
    public class TangleSerializerAttribute : Attribute {
    }

    [AttributeUsage(AttributeTargets.Method, Inherited = false, AllowMultiple = false)]
    public class TangleDeserializerAttribute : Attribute {
    }

    internal unsafe delegate bool GetKeyOfEntryFunc (BTreeValue * pEntry, ushort keyType, out TangleKey key);

    public unsafe struct SerializationContext {
        private bool BytesProvided, StreamInUse;
        private ArraySegment<byte> _Bytes;
        private readonly MemoryStream _Stream;
        private readonly BTreeValue * ValuePointer;
        private readonly ushort KeyType;
        private readonly GetKeyOfEntryFunc GetKeyOfEntry;
        private TangleKey _Key;
        private bool _KeyCached;
        
        internal SerializationContext (GetKeyOfEntryFunc getKeyOfEntry, BTreeValue * pEntry, ushort keyType, MemoryStream stream) {
            GetKeyOfEntry = getKeyOfEntry;
            ValuePointer = pEntry;
            KeyType = keyType;
            _Key = default(TangleKey);
            _KeyCached = false;
            _Stream = stream;
            BytesProvided = false;
            StreamInUse = false;
            _Bytes = default(ArraySegment<byte>);
        }

        public void SetResult (byte[] bytes) {
            SetResult(new ArraySegment<byte>(bytes));
        }

        public void SetResult (ArraySegment<byte> bytes) {
            if (StreamInUse)
                throw new InvalidOperationException("You are already using this context's Stream");
            if (BytesProvided)
                throw new InvalidOperationException("You already provided an ArraySegment");

            BytesProvided = true;
            _Bytes = bytes;
        }

        public TangleKey Key {
            get {
                if (!_KeyCached)
                    _KeyCached = GetKeyOfEntry(ValuePointer, KeyType, out _Key);
                
                if (!_KeyCached)
                    throw new InvalidDataException();

                return _Key;
            }
        }

        public Stream Stream {
            get {
                if (BytesProvided)
                    throw new InvalidOperationException("You already provided an ArraySegment");

                StreamInUse = true;
                return _Stream;
            }
        }

        internal ArraySegment<byte> Bytes {
            get {
                if (BytesProvided)
                    return _Bytes;
                else
                    return new ArraySegment<byte>(_Stream.GetBuffer(), 0, (int)_Stream.Length);
            }
        }

        public void SerializeValue<U> (U input) {
            SerializeValue<U>(ref input);
        }

        public void SerializeValue<U> (ref U input) {
            SerializeValue<U>(Defaults<U>.Serializer, ref input);
        }

        public void SerializeValue<U> (Serializer<U> serializer, U input) {
            SerializeValue<U>(serializer, ref input);
        }

        public void SerializeValue<U> (Serializer<U> serializer, ref U input) {
            var subContext = new SerializationContext(GetKeyOfEntry, ValuePointer, KeyType, _Stream);
            serializer(ref subContext, ref input);
            if (subContext.BytesProvided)
                _Stream.Write(subContext._Bytes.Array, subContext._Bytes.Offset, subContext._Bytes.Count);
        }
    }

    public unsafe struct DeserializationContext {
        private UnmanagedMemoryStream _Stream;
        private readonly BTreeValue * ValuePointer;
        private readonly GetKeyOfEntryFunc GetKeyOfEntry;
        private TangleKey _Key;
        private bool _KeyCached;

        public readonly byte* Source;
        public readonly uint SourceLength;

        internal DeserializationContext (GetKeyOfEntryFunc getKeyOfEntry, BTreeValue * pEntry, byte * source, uint sourceLength) {
            GetKeyOfEntry = getKeyOfEntry;
            _Key = default(TangleKey);
            _KeyCached = false;
            ValuePointer = pEntry;
            Source = source;
            SourceLength = sourceLength;
            _Stream = null;
        }

        public TangleKey Key {
            get {
                if (!_KeyCached)
                    _KeyCached = GetKeyOfEntry(ValuePointer, ValuePointer->KeyType, out _Key);
                
                if (!_KeyCached)
                    throw new InvalidDataException();

                return _Key;
            }
        }

        public UnmanagedMemoryStream Stream {
            get {
                if (_Stream == null)
                    _Stream = new UnmanagedMemoryStream(Source, SourceLength, SourceLength, FileAccess.Read);

                return _Stream;
            }
        }

        public void DeserializeValue<U> (uint offset, out U output) {
            DeserializeValue<U>(offset, SourceLength - offset, out output);
        }

        public void DeserializeValue<U> (uint offset, uint length, out U output) {
            DeserializeValue<U>(Defaults<U>.Deserializer, offset, length, out output);
        }

        public void DeserializeValue<U> (Deserializer<U> deserializer, uint offset, out U output) {
            DeserializeValue<U>(deserializer, offset, SourceLength - offset, out output);
        }

        public void DeserializeValue<U> (Deserializer<U> deserializer, uint offset, uint length, out U output) {
            var subContext = new DeserializationContext(GetKeyOfEntry, ValuePointer, Source + offset, length);
            try {
                deserializer(ref subContext, out output);
            } finally {
                subContext.Dispose();
            }
        }

        internal void Dispose () {
            if (_Stream != null)
                _Stream.Dispose();
        }
    }

    public static class ImmutableBufferPool {
        public unsafe static ArraySegment<byte> GetBytes (int value) {
            var result = ImmutableArrayPool<byte>.Allocate(4);

            fixed (byte * pBuffer = result.Array)
                *(int *)(pBuffer + result.Offset) = value;

            return result;
        }

        public unsafe static ArraySegment<byte> GetBytes (uint value) {
            var result = ImmutableArrayPool<byte>.Allocate(4);

            fixed (byte * pBuffer = result.Array)
                *(uint *)(pBuffer + result.Offset) = value;

            return result;
        }

        public unsafe static ArraySegment<byte> GetBytes (long value) {
            var result = ImmutableArrayPool<byte>.Allocate(8);

            fixed (byte * pBuffer = result.Array)
                *(long *)(pBuffer + result.Offset) = value;

            return result;
        }

        public unsafe static ArraySegment<byte> GetBytes (ulong value) {
            var result = ImmutableArrayPool<byte>.Allocate(8);

            fixed (byte * pBuffer = result.Array)
                *(ulong *)(pBuffer + result.Offset) = value;

            return result;
        }

        public static ArraySegment<byte> GetBytes (string value, Encoding encoding) {
            var length = encoding.GetByteCount(value);
            var result = ImmutableArrayPool<byte>.Allocate(length);

            encoding.GetBytes(value, 0, value.Length, result.Array, result.Offset);

            return result;
        }
    }

    public static class ImmutableArrayPool<T> {
        private class State {
            public readonly T[] Buffer;
            public int ElementsUsed;

            public State (T[] buffer) {
                Buffer = buffer;
                ElementsUsed = 0;
            }
        }

        public const int MaxSizeBytes = 256 * 1024;

        public static readonly int Capacity;

        private readonly static ThreadLocal<State> ThreadLocal = new ThreadLocal<State>();

        static ImmutableArrayPool () {
            var itemSize = Marshal.SizeOf(typeof(T));
            Capacity = MaxSizeBytes / itemSize;
        }

        public static ArraySegment<T> Allocate (int count) {
            if (count > Capacity)
                return new ArraySegment<T>(new T[count], 0, count);

            var data = ThreadLocal.Value;

            if ((data == null) || (data.ElementsUsed >= Capacity - count)) {
                data = ThreadLocal.Value = new State(new T[Capacity]);
            }

            var offset = data.ElementsUsed;
            data.ElementsUsed += count;

            return new ArraySegment<T>(data.Buffer, offset, count);
        }
    }
}

namespace Squared.Data.Mangler.Serialization {
    /// <summary>
    /// Handles converting a single value from the Tangle into raw binary for storage.
    /// </summary>
    public delegate void Serializer<T> (ref SerializationContext context, ref T input);
    /// <summary>
    /// Handles converting a single stored value from raw binary back into its native format, when the Tangle is loading it from storage.
    /// </summary>
    public unsafe delegate void Deserializer<T> (ref DeserializationContext context, out T output);

    public class StringSerializer {
        public readonly Encoding Encoding;
        public readonly Deserializer<string> Deserialize;
        public readonly Serializer<string> Serialize;

        public StringSerializer (Encoding encoding = null) {
            Encoding = encoding ?? Encoding.UTF8;
            Serialize = _Serialize;
            Deserialize = _Deserialize;
        }

        private void _Serialize (ref SerializationContext context, ref string input) {
            var bytes = Encoding.GetBytes(input);
            context.Stream.Write(bytes, 0, bytes.Length);
        }

        private unsafe void _Deserialize (ref DeserializationContext context, out string output) {
            output = new String((sbyte *)context.Source, 0, (int)context.SourceLength, Encoding);
        }
    }

    public static class BlittableSerializer<T>
        where T : struct {

        public static readonly uint Size;
        public static readonly Serializer<T> Serialize;
        public static readonly Deserializer<T> Deserialize;

        static BlittableSerializer () {
            Size = (uint)Marshal.SizeOf(typeof(T));
            Serialize = _Serialize;
            Deserialize = _Deserialize;
        }

        static unsafe void _Serialize (ref SerializationContext context, ref T input) {
            var buffer = ImmutableArrayPool<byte>.Allocate((int)Size);
            fixed (byte * pBuffer = buffer.Array)
                Unsafe<T>.StructureToPtr(ref input, pBuffer + buffer.Offset, Size);

            context.Stream.Write(buffer.Array, buffer.Offset, (int)Size);
        }

        static unsafe void _Deserialize (ref DeserializationContext context, out T output) {
            Unsafe<T>.PtrToStructure(context.Source, out output, context.SourceLength);
        }
    }

    public static class Defaults<T> {
        public static Serializer<T> Serializer = SerializeToXml;
        public static Deserializer<T> Deserializer = DeserializeFromXml;

        unsafe static Defaults () {
            var t = typeof(T);
            var tsa = typeof(TangleSerializerAttribute);
            var tda = typeof(TangleDeserializerAttribute);

            foreach (var method in t.GetMethods(
                System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.Public | 
                System.Reflection.BindingFlags.NonPublic
            )) {
                var sa = method.GetCustomAttributes(tsa, true);
                if (sa.Length == 1)
                    Serializer = (Serializer<T>)(Delegate.CreateDelegate(typeof(Serializer<T>), method, true)) ?? Serializer;

                sa = method.GetCustomAttributes(tda, true);
                if (sa.Length == 1)
                    Deserializer = (Deserializer<T>)(Delegate.CreateDelegate(typeof(Deserializer<T>), method, true)) ?? Deserializer;
            }
        }

        public static void SerializeToXml (ref SerializationContext context, ref T input) {
            var ser = new XmlSerializer(typeof(T));
            ser.Serialize(context.Stream, input);
        }

        public static void DeserializeFromXml (ref DeserializationContext context, out T output) {
            var ser = new XmlSerializer(typeof(T));
            output = (T)ser.Deserialize(context.Stream);
        }
    }
}
