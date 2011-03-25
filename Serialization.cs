using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Xml.Serialization;

namespace Squared.Data.Mangler {
    public class TangleSerializerAttribute : Attribute {
    }

    public class TangleDeserializerAttribute : Attribute {
    }
}

namespace Squared.Data.Mangler.Serialization {
    /// <summary>
    /// Handles converting a single value from the Tangle into raw binary for storage.
    /// </summary>
    public delegate void Serializer<T> (ref T input, Stream output);
    /// <summary>
    /// Handles converting a single stored value from raw binary back into its native format, when the Tangle is loading it from storage.
    /// </summary>
    public unsafe delegate void Deserializer<T> (byte * input, uint inputLength, out T output);
    /// <summary>
    /// Handles converting a single stored value from raw binary back into its native format, when the Tangle is loading it from storage.
    /// </summary>
    public delegate void StreamDeserializer<T> (Stream input, out T output);

    public class StringSerializer {
        public readonly Encoding Encoding;

        public StringSerializer (Encoding encoding = null) {
            Encoding = encoding ?? Encoding.UTF8;
        }

        public void Serialize (ref string input, Stream output) {
            var bytes = Encoding.GetBytes(input);
            output.Write(bytes, 0, bytes.Length);
        }

        public unsafe void Deserialize (byte * input, uint inputLength, out string output) {
            output = new String((sbyte *)input, 0, (int)inputLength, Encoding);
        }
    }

    class StreamDeserializerAdapter<T> {
        public readonly StreamDeserializer<T> Deserializer;

        public StreamDeserializerAdapter (StreamDeserializer<T> deserializer) {
            Deserializer = deserializer;
        }

        public unsafe void Deserialize (byte * input, uint inputLength, out T output) {
            using (var stream = new UnmanagedMemoryStream(input, inputLength, inputLength, FileAccess.Read))
                Deserializer(stream, out output);
        }
    }

    public static class Defaults<T> {
        public static Serializer<T> Serializer = SerializeToXml;
        public unsafe static Deserializer<T> Deserializer = DeserializeFromXml;

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
                if (sa.Length == 1) {
                    var native = (Deserializer<T>)(Delegate.CreateDelegate(typeof(Deserializer<T>), method, false));
                    if (native != null)
                        Deserializer = native;
                    else {
                        var streamBased = (StreamDeserializer<T>)(Delegate.CreateDelegate(typeof(StreamDeserializer<T>), method, true));
                        if (streamBased != null) {
                            var adapter = new StreamDeserializerAdapter<T>(streamBased);
                            Deserializer = adapter.Deserialize;
                        }
                    }
                }
            }
        }

        public static void SerializeToXml (ref T input, Stream output) {
            var ser = new XmlSerializer(typeof(T));
            ser.Serialize(output, input);
        }

        public unsafe static void DeserializeFromXml (byte * input, uint inputLength, out T output) {
            using (var stream = new UnmanagedMemoryStream(input, inputLength, inputLength, FileAccess.Read)) {
                var ser = new XmlSerializer(typeof(T));
                output = (T)ser.Deserialize(stream);
            }
        }
    }
}
