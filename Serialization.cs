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

    public unsafe struct SerializationContext {
        private bool BytesProvided, StreamInUse;
        private ArraySegment<byte> _Bytes;
        private readonly MemoryStream _Stream;

        public SerializationContext (MemoryStream stream) {
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
            var subContext = new SerializationContext(_Stream);
            serializer(ref subContext, ref input);
            if (subContext.BytesProvided)
                _Stream.Write(subContext._Bytes.Array, subContext._Bytes.Offset, subContext._Bytes.Count);
        }
    }

    public unsafe struct DeserializationContext {
        private UnmanagedMemoryStream _Stream;

        public readonly byte * Source;
        public readonly uint SourceLength;

        public DeserializationContext (byte * source, uint sourceLength) {
            Source = source;
            SourceLength = sourceLength;
            _Stream = null;
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
            var subContext = new DeserializationContext(Source + offset, length);
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

        public StringSerializer (Encoding encoding = null) {
            Encoding = encoding ?? Encoding.UTF8;
        }

        public void Serialize (ref SerializationContext context, ref string input) {
            var bytes = Encoding.GetBytes(input);
            context.Stream.Write(bytes, 0, bytes.Length);
        }

        public unsafe void Deserialize (ref DeserializationContext context, out string output) {
            output = new String((sbyte *)context.Source, 0, (int)context.SourceLength, Encoding);
        }
    }

    public static class BlittableSerializer<T>
        where T : struct {

        public static readonly uint Size;

        static BlittableSerializer () {
            Size = (uint)Marshal.SizeOf(typeof(T));
        }

        public static unsafe void Serialize (ref SerializationContext context, ref T input) {
            var buffer = new byte[Size];
            fixed (byte * pBuffer = buffer)
                Unsafe<T>.StructureToPtr(ref input, pBuffer, Size);

            context.Stream.Write(buffer, 0, (int)Size);
        }

        public static unsafe void Deserialize (ref DeserializationContext context, out T output) {
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
