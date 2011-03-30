using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Squared.Data.Mangler.Internal;
using Squared.Task;

namespace Squared.Data.Mangler {
    public abstract class IndexBase<TValue> : IDisposable {
        internal abstract void OnValueRemoved (TangleKey key, ref TValue oldValue);
        internal abstract void OnValueAdded (TangleKey key, ref TValue newValue);

        public abstract void Dispose ();
    }

    internal struct IndexFunctionAdapter<TIndexKey, TValue> : IEnumerable<TIndexKey>, IEnumerator<TIndexKey> {
        public readonly IndexFunc<TIndexKey, TValue> Function;
        public readonly TValue Input;
        private bool Advanced;

        public IndexFunctionAdapter (IndexFunc<TIndexKey, TValue> function, ref TValue input) {
            Function = function;
            Input = input;
            Advanced = false;
        }

        IEnumerator<TIndexKey> IEnumerable<TIndexKey>.GetEnumerator () {
            return this;
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator () {
            return this;
        }

        TIndexKey IEnumerator<TIndexKey>.Current {
            get {
                return Function(Input);
            }
        }

        void IDisposable.Dispose () {
        }

        object System.Collections.IEnumerator.Current {
            get {
                return Function(Input);
            }
        }

        bool System.Collections.IEnumerator.MoveNext () {
            if (Advanced)
                return false;

            Advanced = true;
            return true;
        }

        void System.Collections.IEnumerator.Reset () {
            Advanced = false;
        }
    }

    public partial class Index<TIndexKey, TValue> : IndexBase<TValue> {
        public readonly Tangle<TValue> Tangle;
        public readonly string Name;

        public readonly IndexFunc<TIndexKey, TValue> IndexFunction;
        public readonly IndexMultipleFunc<TIndexKey, TValue> IndexMultipleFunction;

        private readonly BTree BTree;
        private readonly TangleKeyConverter<TIndexKey> KeyConverter;

        protected Index (Tangle<TValue> tangle, string name, Delegate function) {
            IndexFunction = function as IndexFunc<TIndexKey, TValue>;
            IndexMultipleFunction = function as IndexMultipleFunc<TIndexKey, TValue>;

            if ((IndexFunction == null) && (IndexMultipleFunction == null))
                throw new InvalidOperationException("An index must have either an IndexFunc or IndexMultipleFunc");

            Tangle = tangle;
            Name = name;
            KeyConverter = TangleKey.GetConverter<TIndexKey>();

            IndexBase<TValue> temp;
            if (tangle.Indices.TryGetValue(name, out temp))
                throw new InvalidOperationException("An index with that name already exists");

            BTree = new BTree(tangle.Storage, Name + "_");

            tangle.Indices.Add(name, this);

            if (tangle.Count != BTree.MutationSentinel)
                Populate();
        }

        protected void Populate () {
            long nodeCount = Tangle.NodeCount;

            for (long i = 0; i < nodeCount; i++) {
                foreach (var kvp in Tangle.InternalEnumerateNode(i)) {
                    var value = kvp.Value;
                    UpdateIndexForEntry(kvp.Key, ref value, true);
                }
            }
        }

        public static Future<Index<TIndexKey, TValue>> Create (Tangle<TValue> tangle, string name, IndexFunc<TIndexKey, TValue> function) {
            return tangle.QueueWorkItem(new CreateThunk(name, function));
        }

        public static Future<Index<TIndexKey, TValue>> Create (Tangle<TValue> tangle, string name, IndexMultipleFunc<TIndexKey, TValue> function) {
            return tangle.QueueWorkItem(new CreateThunk(name, function));
        }

        public Future<TangleKey> FindOne (TIndexKey value) {
            var key = KeyConverter(value);
            return Tangle.QueueWorkItem(new FindOneThunk(this, key));
        }

        public Future<TValue> GetOne (TIndexKey value) {
            var key = KeyConverter(value);
            return Tangle.QueueWorkItem(new GetOneThunk(this, key));
        }

        public Future<TangleKey[]> Find (TIndexKey value) {
            var key = KeyConverter(value);
            return Tangle.QueueWorkItem(new FindThunk(this, key));
        }

        public Future<TValue[]> Get (TIndexKey value) {
            var key = KeyConverter(value);
            return Tangle.QueueWorkItem(new GetThunk(this, key));
        }

        internal unsafe void UpdateIndexForEntry (TangleKey key, ref TValue value, bool add) {
            long nodeIndex;
            uint valueIndex;

            IEnumerable<TIndexKey> sequence;

            if (IndexFunction != null)
                sequence = new IndexFunctionAdapter<TIndexKey, TValue>(
                    IndexFunction, ref value
                );
            else
                sequence = IndexMultipleFunction(value);

            foreach (var synthesizedValue in sequence) {
                TangleKey synthesizedKey = KeyConverter(synthesizedValue);

                bool foundExisting = BTree.FindKey(synthesizedKey, true, out nodeIndex, out valueIndex);

                StreamRange range;
                if (foundExisting) {
                    range = BTree.AccessNode(nodeIndex, true);
                } else if (add) {
                    range = BTree.PrepareForInsert(nodeIndex, valueIndex);
                } else {
                    throw new InvalidOperationException();
                }

                using (range) {
                    var pEntry = BTree.LockValue(range, valueIndex, foundExisting ? synthesizedKey.OriginalTypeId : (ushort)0);

                    HashSet<TangleKey> keys;
                    if (foundExisting) {
                        BTree.ReadData(pEntry, synthesizedKey.OriginalTypeId, DeserializeKeys, out keys);
                    } else {
                        BTree.WriteNewKey(pEntry, synthesizedKey);
                        keys = new HashSet<TangleKey>();
                    }

                    if (add)
                        keys.Add(key);
                    else
                        keys.Remove(key);

                    ArraySegment<byte> data = BTree.Serialize(
                        pEntry, SerializeKeys, synthesizedKey.OriginalTypeId, ref keys
                    );

                    BTree.WriteData(pEntry, data);

                    BTree.UnlockValue(pEntry, synthesizedKey.OriginalTypeId);

                    if (foundExisting)
                        BTree.UnlockNode(range);
                    else
                        BTree.FinalizeInsert(range);
                }
            }

            if (add)
                BTree.MutationSentinel += 1;
            else
                BTree.MutationSentinel -= 1;
        }

        internal override void OnValueRemoved (TangleKey key, ref TValue oldValue) {
            UpdateIndexForEntry(key, ref oldValue, false);
        }

        internal override void OnValueAdded (TangleKey key, ref TValue newValue) {
            UpdateIndexForEntry(key, ref newValue, true);
        }

        private unsafe static void SerializeKeys (ref SerializationContext context, ref HashSet<TangleKey> input) {
            var countBytes = ImmutableBufferPool.GetBytes(input.Count);
            context.Stream.Write(countBytes.Array, countBytes.Offset, countBytes.Count);

            foreach (var key in input) {
                countBytes = ImmutableBufferPool.GetBytes(key.Data.Count);
                context.Stream.Write(countBytes.Array, countBytes.Offset, countBytes.Count);
                var typeBytes = ImmutableBufferPool.GetBytes(key.OriginalTypeId);
                context.Stream.Write(typeBytes.Array, typeBytes.Offset, typeBytes.Count);
                context.Stream.Write(key.Data.Array, key.Data.Offset, key.Data.Count);
            }
        }

        private unsafe static void DeserializeKeys (ref DeserializationContext context, out HashSet<TangleKey> output) {
            if (context.SourceLength < 4)
                throw new InvalidDataException();

            int count = *(int *)(context.Source);
            output = new HashSet<TangleKey>();
            uint offset = 4;

            for (int i = 0; i < count; i++) {
                if (context.SourceLength < (offset + 6))
                    throw new InvalidDataException();

                int keyLength = *(int *)(context.Source + offset);
                ushort keyType = *(ushort *)(context.Source + offset + 4);

                offset += 6;
                if (context.SourceLength < (offset + keyLength))
                    throw new InvalidDataException();

                var keyBytes = ImmutableArrayPool<byte>.Allocate(keyLength);
                Unsafe.ReadBytes(context.Source, offset, keyBytes.Array, keyBytes.Offset, (uint)keyBytes.Count);

                output.Add(new TangleKey(keyBytes, keyType));
                offset += (uint)keyLength;
            }
        }

        public override void Dispose () {
            BTree.Dispose();
        }
    }
}
