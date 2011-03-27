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
using System.Linq.Expressions;
using System.Text;
using System.IO;
using Squared.Data.Mangler.Internal;

namespace Squared.Data.Mangler {
    public struct TangleKey : IComparable<TangleKey>, IEquatable<TangleKey> {
        private static readonly Dictionary<Type, Delegate> Converters = new Dictionary<Type, Delegate>();
        private static readonly Dictionary<ushort, Type> TypeIdToType = new Dictionary<ushort, Type>();
        private static readonly Dictionary<Type, ushort> TypeToTypeId = new Dictionary<Type, ushort>();

        static TangleKey () {
            Converters[typeof(TangleKey)] = (Func<TangleKey, TangleKey>)((key) => key);

            RegisterType<string>();
            RegisterType<byte[]>();
            RegisterType<uint>();
            RegisterType<int>();
            RegisterType<ulong>();
            RegisterType<long>();
        }

        private static void RegisterType<T> (bool autoConverter = true) {
            if (TypeToTypeId.Count >= (ushort.MaxValue - 2))
                throw new InvalidOperationException("Too many registered types");

            var type = typeof(T);
            ushort id = (byte)(TypeToTypeId.Count + 1);
            TypeToTypeId[type] = id;
            TypeIdToType[id] = type;

            if (autoConverter) {
                var constructor = typeof(TangleKey).GetConstructor(new Type[] { type });
                var parameter = Expression.Parameter(type, "value");
                var expression = Expression.New(constructor, new[] { parameter });
                var converterType = typeof(Func<T, TangleKey>);
                var converterExpression = Expression.Lambda(converterType, expression, parameter);
                var converter = converterExpression.Compile();
                Converters[type] = converter;
            }
        }

        public static void RegisterKeyType<T> (Func<T, TangleKey> converter) {
            RegisterType<T>(autoConverter: false);
            Converters[typeof(T)] = converter;
        }

        public static Func<T, TangleKey> GetConverter<T> () {
            Delegate converter;
            if (!Converters.TryGetValue(typeof(T), out converter))
                throw new InvalidOperationException(String.Format("Type '{0}' is not convertible to TangleKey", typeof(T).Name));

            return (Func<T, TangleKey>)converter;
        }

        public readonly ushort OriginalTypeId;
        public readonly ArraySegment<byte> Data;

        public TangleKey (uint key)
            : this(ImmutableBufferPool.GetBytes(key), typeof(uint)) {
        }

        public TangleKey (ulong key)
            : this(ImmutableBufferPool.GetBytes(key), typeof(ulong)) {
        }

        public TangleKey (int key)
            : this(ImmutableBufferPool.GetBytes(key), typeof(int)) {
        }

        public TangleKey (long key)
            : this(ImmutableBufferPool.GetBytes(key), typeof(long)) {
        }

        public TangleKey (string key)
            : this(ImmutableBufferPool.GetBytes(key, Encoding.UTF8), typeof(string)) {
        }

        public TangleKey (byte[] array)
            : this(array, 0, array.Length, TypeToTypeId[typeof(byte[])]) {
        }

        public TangleKey (byte[] array, int offset, int count)
            : this(array, offset, count, TypeToTypeId[typeof(string)]) {
        }

        public TangleKey (byte[] array, ushort originalType)
            : this(array, 0, array.Length, originalType) {
        }

        public TangleKey (byte[] array, int offset, int count, Type originalType)
            : this(new ArraySegment<byte>(array, offset, count), originalType) {
        }

        public TangleKey (byte[] array, int offset, int count, ushort originalType)
            : this(new ArraySegment<byte>(array, offset, count), originalType) {
        }

        public TangleKey (ArraySegment<byte> data, Type originalType)
            : this(data, TypeToTypeId[originalType]) {
        }

        public TangleKey (ArraySegment<byte> data, ushort originalType) {
            if (data.Count >= ushort.MaxValue)
                throw new InvalidDataException("Key too long");
            if (originalType == 0)
                throw new InvalidDataException("Invalid key type");

            Data = data;
            OriginalTypeId = originalType;
        }

        public Type OriginalType {
            get {
                return TypeIdToType[OriginalTypeId];
            }
        }

        public object Value {
            get {
                var type = OriginalType;

                if (type == typeof(string)) {
                    return Encoding.ASCII.GetString(Data.Array, Data.Offset, Data.Count);
                } else if (type == typeof(int)) {
                    return BitConverter.ToInt32(Data.Array, Data.Offset);
                } else if (type == typeof(uint)) {
                    return BitConverter.ToUInt32(Data.Array, Data.Offset);
                } else if (type == typeof(long)) {
                    return BitConverter.ToInt64(Data.Array, Data.Offset);
                } else if (type == typeof(ulong)) {
                    return BitConverter.ToUInt64(Data.Array, Data.Offset);
                } else /* if (type == typeof(byte[])) */ {
                    return Data;
                }
            }
        }

        public static implicit operator TangleKey (string key) {
            return new TangleKey(key);
        }

        public static implicit operator TangleKey (uint key) {
            return new TangleKey(key);
        }

        public static implicit operator TangleKey (int key) {
            return new TangleKey(key);
        }

        public static implicit operator TangleKey (ulong key) {
            return new TangleKey(key);
        }

        public static implicit operator TangleKey (long key) {
            return new TangleKey(key);
        }

        public override string ToString () {
            var value = Value;

            if (value is ArraySegment<byte>) {
                var sb = new StringBuilder();
                for (int i = 0; i < Data.Count; i++)
                    sb.AppendFormat("{0:X2}", Data.Array[i + Data.Offset]);
                return sb.ToString();
            } else {
                return value.ToString();
            }
        }

        private static unsafe int CompareData (ArraySegment<byte> lhs, ArraySegment<byte> rhs) {
            int result;
            uint compareLength = (uint)Math.Min(lhs.Count, rhs.Count);

            fixed (byte * pLhs = &lhs.Array[lhs.Offset])
            fixed (byte * pRhs = &rhs.Array[rhs.Offset])
                result = Native.memcmp(pLhs, pRhs, new UIntPtr(compareLength));

            if (result == 0) {
                if (lhs.Count > rhs.Count)
                    result = 1;
                else if (rhs.Count > lhs.Count)
                    result = -1;
            }

            return result;
        }

        public int CompareTo (TangleKey rhs) {
            int result = OriginalTypeId.CompareTo(rhs.OriginalTypeId);
            if (result == 0)
                result = CompareData(Data, rhs.Data);

            return result;
        }

        public bool Equals (TangleKey other) {
            return CompareTo(other) == 0;
        }

        public override bool Equals (object other) {
            if (other is TangleKey)
                return this.Equals((TangleKey)other);
            else
                return base.Equals(other);
        }

        // FNV hash algorithm
        public override int GetHashCode () {
            unchecked {
                const int p = 16777619;
                int hash = -2128831035;

                for (int i = 0, c = Data.Count; i < c; i++)
                    hash = (hash ^ Data.Array[i + Data.Offset]) * p;

                hash += hash << 13;
                hash ^= hash >> 7;
                hash += hash << 3;
                hash ^= hash >> 17;
                hash += hash << 5;

                return hash;
            }            
        }
    }
}
