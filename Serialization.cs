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
    public delegate void Deserializer<T> (Stream input, out T output);

    public static class Defaults<T> {
        public static Serializer<T> Serializer = SerializeToXml;
        public static Deserializer<T> Deserializer = DeserializeFromXml;

        static Defaults () {
            var t = typeof(T);
            var tsa = typeof(TangleSerializerAttribute);
            var tda = typeof(TangleDeserializerAttribute);

            foreach (var method in t.GetMethods(
                System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.Public | 
                System.Reflection.BindingFlags.NonPublic
            )) {
                var sa = method.GetCustomAttributes(tsa, true);
                if (sa.Length == 1)
                    Serializer = (Delegate.CreateDelegate(typeof(Serializer<T>), method) as Serializer<T>) ?? Serializer;

                sa = method.GetCustomAttributes(tda, true);
                if (sa.Length == 1)
                    Deserializer = (Delegate.CreateDelegate(typeof(Deserializer<T>), method) as Deserializer<T>) ?? Deserializer;
            }
        }

        public static void SerializeToXml (ref T input, Stream output) {
            var ser = new XmlSerializer(typeof(T));
            ser.Serialize(output, input);
        }

        public static void DeserializeFromXml (Stream input, out T output) {
            var ser = new XmlSerializer(typeof(T));
            output = (T)ser.Deserialize(input);
        }
    }
}
