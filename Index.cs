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
using System.IO;
using System.Linq;
using System.Text;
using Squared.Data.Mangler.Internal;
using Squared.Task;

namespace Squared.Data.Mangler {
    public abstract class IndexBase<TValue> : IDisposable {
        internal abstract void OnValueRemoved (TangleKey key, ref TValue oldValue);
        internal abstract void OnValueAdded (TangleKey key, ref TValue newValue);

        internal abstract void Clear ();

        public abstract void Dispose ();
    }

    internal struct IndexFunctionAdapter<TIndexKey, TValue> : IEnumerable<TIndexKey>, IEnumerator<TIndexKey> {
        public readonly IndexFunc<TIndexKey, TValue> Function;
        public TValue Input;
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
                return Function(ref Input);
            }
        }

        void IDisposable.Dispose () {
        }

        object System.Collections.IEnumerator.Current {
            get {
                return Function(ref Input);
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

        public Future<TangleKey[]> Find (IEnumerable<TIndexKey> values) {
            return Tangle.QueueWorkItem(new FindMultipleThunk(
                this, from value in values select KeyConverter(value)
            ));
        }

        public Future<TValue[]> Get (TIndexKey value) {
            var key = KeyConverter(value);
            return Tangle.QueueWorkItem(new GetThunk(this, key));
        }

        public Future<TValue[]> Get (IEnumerable<TIndexKey> values) {
            return Tangle.QueueWorkItem(new GetMultipleThunk(
                this, from value in values select KeyConverter(value)
            ));
        }

        /// <summary>
        /// Reads every key from the index, in no particular order.
        /// </summary>
        /// <returns>A future that will contain the retrieved keys.</returns>
        public Future<TangleKey[]> GetAllKeys () {
            return Tangle.QueueWorkItem(new GetAllKeysThunk(this));
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

                long minimumSize;
                using (range) {
                    if (!foundExisting)
                        BTree.WriteNewKey(range, valueIndex, synthesizedKey);

                    minimumSize = BTree.GetValueDataTotalBytes(
                        range, valueIndex, 
                        foundExisting ? synthesizedKey.OriginalTypeId : (ushort)0
                    );
                }

                if (foundExisting)
                    BTree.UnlockNode(range);
                else
                    BTree.FinalizeInsert(range);

                if (add) {
                    // Ensure we will have enough room to insert this key, if necessary
                    if (minimumSize == 0)
                        minimumSize = 4;

                    minimumSize += 6 + key.Data.Count;
                }

                BTreeValue * pValue;
                ushort lockedKeyType;

                fixed (byte * pKeyBytes = &key.Data.Array[key.Data.Offset])
                using (var indexRange = BTree.LockValue(nodeIndex, valueIndex, minimumSize, out pValue, out lockedKeyType))
                using (var dataRange = BTree.DataStream.AccessRange(pValue->DataOffset, (uint)minimumSize)) {
                    if ((pValue->DataLength < 4)) {
                        pValue->ExtraDataBytes -= (4 - pValue->DataLength);
                        pValue->DataLength = 4;
                        *(int *)dataRange.Pointer = 0;
                    }

                    int numKeys = *(int *)dataRange.Pointer;
                    uint offset = 4;

                    uint? keyPosition = null, totalKeySize = null;
                    for (int i = 0; i < numKeys; i++) {                       
                        int keyLength = *(int *)(dataRange.Pointer + offset);
                        offset += 4;
                        ushort comparisonKeyType = *(ushort *)(dataRange.Pointer + offset);
                        offset += 2;

                        if ((comparisonKeyType == key.OriginalTypeId) && (Native.memcmp(
                            dataRange.Pointer + offset, pKeyBytes, 
                            new UIntPtr((uint)Math.Min(key.Data.Count, keyLength))
                        ) == 0)) {
                            keyPosition = offset - 6;
                            totalKeySize = (uint)(6 + keyLength);
                            break;
                        }

                        offset += (uint)keyLength;
                    }

                    if (add) {
                        if (!keyPosition.HasValue) {
                            // Add the key at the end of the list
                            var insertPosition = pValue->DataLength;
                            if ((pValue->DataLength + pValue->ExtraDataBytes) < (insertPosition + 6 + key.Data.Count))
                                throw new InvalidDataException();

                            *(int *)(dataRange.Pointer + insertPosition) = key.Data.Count;
                            *(ushort *)(dataRange.Pointer + insertPosition + 4) = key.OriginalTypeId;
                            Native.memmove(
                                dataRange.Pointer + insertPosition + 6,
                                pKeyBytes, new UIntPtr((uint)key.Data.Count)
                            );

                            pValue->DataLength += (uint)(6 + key.Data.Count);
                            pValue->ExtraDataBytes -= (uint)(6 + key.Data.Count);

                            *(int *)dataRange.Pointer += 1;
                        }
                    } else if (keyPosition.HasValue) {
                        // Remove the key by moving the items after it back in the list

                        var moveSize = dataRange.Size - (keyPosition.Value + totalKeySize);                        
                        if (moveSize > 0)
                            Native.memmove(
                                dataRange.Pointer + keyPosition.Value,
                                dataRange.Pointer + keyPosition.Value + totalKeySize.Value,
                                new UIntPtr((uint)moveSize)
                            );

                        pValue->DataLength -= (uint)(6 + key.Data.Count);
                        pValue->ExtraDataBytes += (uint)(6 + key.Data.Count);

                        *(int *)dataRange.Pointer -= 1;
                    }

                    BTree.UnlockValue(pValue, synthesizedKey.OriginalTypeId);
                    BTree.UnlockNode(indexRange);
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

        internal override void Clear () {
            BTree.Clear();
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
