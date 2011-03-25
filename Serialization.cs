using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.IO;
using System.Xml.Serialization;
using Squared.Data.Mangler.Internal;

namespace Squared.Data.Mangler {
    public class TangleSerializerAttribute : Attribute {
    }

    public class TangleDeserializerAttribute : Attribute {
    }

    public unsafe struct SerializationContext<T> {
        private readonly Tangle<T> Tangle;

        public readonly MemoryStream Stream;

        public SerializationContext (Tangle<T> tangle, MemoryStream stream) {
            Tangle = tangle;
            Stream = stream;
        }
    }

    public unsafe struct DeserializationContext<T> {
        private readonly Tangle<T> Tangle;
        public readonly byte * Source;
        public readonly uint SourceLength;

        public DeserializationContext (Tangle<T> tangle, byte * source, uint sourceLength) {
            Tangle = tangle;
            Source = source;
            SourceLength = sourceLength;
        }

        public UnmanagedMemoryStream GetStream () {
            return new UnmanagedMemoryStream(Source, SourceLength, SourceLength, FileAccess.Read);
        }
    }
}

namespace Squared.Data.Mangler.Serialization {
    /// <summary>
    /// Handles converting a single value from the Tangle into raw binary for storage.
    /// </summary>
    public delegate void Serializer<T> (ref SerializationContext<T> context, ref T input);
    /// <summary>
    /// Handles converting a single stored value from raw binary back into its native format, when the Tangle is loading it from storage.
    /// </summary>
    public unsafe delegate void Deserializer<T> (ref DeserializationContext<T> context, out T output);

    public class StringSerializer {
        public readonly Encoding Encoding;

        public StringSerializer (Encoding encoding = null) {
            Encoding = encoding ?? Encoding.UTF8;
        }

        public void Serialize (ref SerializationContext<string> context, ref string input) {
            var bytes = Encoding.GetBytes(input);
            context.Stream.Write(bytes, 0, bytes.Length);
        }

        public unsafe void Deserialize (ref DeserializationContext<string> context, out string output) {
            output = new String((sbyte *)context.Source, 0, (int)context.SourceLength, Encoding);
        }
    }

    public static class BlittableSerializer<T>
        where T : struct {

        public static readonly uint Size;

        static BlittableSerializer () {
            Size = (uint)Marshal.SizeOf(typeof(T));
        }

        public static unsafe void Serialize (ref SerializationContext<T> context, ref T input) {
            var buffer = new byte[Size];
            fixed (byte * pBuffer = buffer)
                Unsafe<T>.StructureToPtr(ref input, pBuffer, Size);

            context.Stream.Write(buffer, 0, (int)Size);
        }

        public static unsafe void Deserialize (ref DeserializationContext<T> context, out T output) {
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
                    Deserializer = (Deserializer<T>)(Delegate.CreateDelegate(typeof(Deserializer<T>), method, false)) ?? Deserializer;
            }
        }

        public static void SerializeToXml (ref SerializationContext<T> context, ref T input) {
            var ser = new XmlSerializer(typeof(T));
            ser.Serialize(context.Stream, input);
        }

        public static void DeserializeFromXml (ref DeserializationContext<T> context, out T output) {
            using (var stream = context.GetStream()) {
                var ser = new XmlSerializer(typeof(T));
                output = (T)ser.Deserialize(stream);
            }
        }
    }
}
