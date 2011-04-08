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
using System.Threading.Tasks;
using Squared.Task;
using System.Threading;
using Squared.Data.Mangler.Internal;
using TaskScheduler = Squared.Task.TaskScheduler;

namespace Squared.Data.Mangler {
    public partial class Tangle<T> {
        public abstract class ThunkBase<U> : IWorkItemWithFuture<T, U>, IDisposable {
            protected Future<U> Future = new Future<U>();
            protected Exception Failure = null;

            protected abstract void OnExecute (Tangle<T> tangle, out U result);

            protected void CompleteEarly (ref U result) {
                Future.Complete(result);
            }

            protected void Fail (Exception ex) {
                Failure = ex;
            }

            public void Execute (Tangle<T> tangle) {
                if (!Future.Disposed)
                    try {
                        U result;
                        OnExecute(tangle, out result);

                        if (!Future.Completed) {
                            if (Failure != null)
                                Future.Fail(Failure);
                            else
                                Future.Complete(result);
                        }
                    } catch (Exception ex) {
                        if (!Future.Disposed && !Future.Completed)
                            try {
                                Future.Fail(ex);
                            } catch {
                            }
                    }

                Dispose();
            }

            Future<U> IWorkItemWithFuture<T, U>.Future {
                get {
                    return Future;
                }
            }

            public virtual void Dispose () {
                Future.Dispose();
                Future = null;
                Failure = null;
            }
        }

        internal abstract class SetThunkBase<U> : ThunkBase<U> {
            public T Value;

            public override void Dispose () {
                base.Dispose();
                Value = default(T);
            }
        }

        private class SetThunk : SetThunkBase<bool>, IReplaceCallback<T> {
            public readonly TangleKey Key;
            public readonly bool ShouldReplace;

            public SetThunk (TangleKey key, ref T value, bool shouldReplace) {
                Key = key;
                Value = value;
                ShouldReplace = shouldReplace;
            }

            bool IReplaceCallback<T>.ShouldReplace (Tangle<T> tangle, ref BTreeValue btreeValue, ushort keyType, ref T newValue) {
                return ShouldReplace;
            }

            protected override void OnExecute (Tangle<T> tangle, out bool result) {
                result = tangle.InternalSet(Key, ref Value, this);
            }
        }

        internal class BatchThunk : SetThunkBase<int>, IReplaceCallback<T> {
            public Batch<T> Batch;
            private bool ShouldReplace;
            private UpdateCallback<T> Callback;
            private DecisionUpdateCallback<T> DecisionCallback;

            public BatchThunk (Batch<T> batch) {
                Batch = batch;
            }

            bool IReplaceCallback<T>.ShouldReplace (Tangle<T> tangle, ref BTreeValue btreeValue, ushort keyType, ref T newValue) {
                T oldValue;
                if (Callback != null) {
                    tangle.ReadData(ref btreeValue, keyType, out oldValue);
                    newValue = Callback(oldValue);
                    return true;
                } else if (DecisionCallback != null) {
                    tangle.ReadData(ref btreeValue, keyType, out oldValue);
                    return DecisionCallback(ref oldValue, ref newValue);
                } else {
                    return ShouldReplace;
                }
            }

            protected override void OnExecute (Tangle<T> tangle, out int result) {
                result = 0;

                var items = Batch.Buffer;
                for (int i = 0, c = Batch.Count; i < c; i++) {
                    ShouldReplace = items[i].AllowReplacement;
                    Callback = items[i].Callback;
                    DecisionCallback = items[i].DecisionCallback;
                    if (tangle.InternalSet(items[i].Key, ref items[i].Value, this))
                        result += 1;
                }
            }

            public override void Dispose () {
                base.Dispose();
                Batch = null;
            }
        }

        private class UpdateThunk : SetThunkBase<bool>, IReplaceCallback<T> {
            public readonly TangleKey Key;
            public readonly UpdateCallback<T> Callback;
            public readonly DecisionUpdateCallback<T> DecisionCallback;

            public UpdateThunk (TangleKey key, ref T value, UpdateCallback<T> callback) {
                Key = key;
                Value = value;
                Callback = callback;
                DecisionCallback = null;
            }

            public UpdateThunk (TangleKey key, ref T value, DecisionUpdateCallback<T> callback) {
                Key = key;
                Value = value;
                Callback = null;
                DecisionCallback = callback;
            }

            bool IReplaceCallback<T>.ShouldReplace (Tangle<T> tangle, ref BTreeValue btreeValue, ushort keyType, ref T newValue) {
                T oldValue;
                tangle.ReadData(ref btreeValue, keyType, out oldValue);

                if (Callback != null) {
                    newValue = Callback(oldValue);
                    return true;
                } else {
                    return DecisionCallback(ref oldValue, ref newValue);
                }
            }

            protected override void OnExecute (Tangle<T> tangle, out bool result) {
                result = tangle.InternalSet(Key, ref Value, this);
            }
        }

        private class GetThunk : ThunkBase<T> {
            public readonly TangleKey Key;

            public GetThunk (TangleKey key) {
                Key = key;
            }

            protected override void OnExecute (Tangle<T> tangle, out T result) {
                if (!tangle.InternalGet(Key, out result))
                    Fail(new KeyNotFoundException(Key));
            }
        }

        private class LockDataThunk : ThunkBase<LockedData> {
            public readonly long NodeIndex;
            public readonly uint ValueIndex;
            public readonly long? MinimumSize;
            public readonly ManualResetEventSlim DisposedSignal;

            public LockDataThunk (long nodeIndex, uint valueIndex, long? minimumSize) {
                NodeIndex = nodeIndex;
                ValueIndex = valueIndex;
                MinimumSize = minimumSize;
                DisposedSignal = new ManualResetEventSlim();
            }

            protected override unsafe void OnExecute (Tangle<T> tangle, out LockedData result) {
                BTreeValue * pValue;
                ushort keyType;

                using (var nodeRange = tangle.BTree.LockValue(NodeIndex, ValueIndex, MinimumSize, out pValue, out keyType))
                using (var dataRange = tangle.BTree.DataStream.AccessRange(pValue->DataOffset, pValue->DataLength + pValue->ExtraDataBytes)) {
                    result = new LockedData(
                        dataRange.Pointer, pValue->DataLength + pValue->ExtraDataBytes, DisposedSignal
                    );

                    // Complete our future early so that the locked region can be consumed
                    //  while we wait on the disposal signal
                    CompleteEarly(ref result);

                    DisposedSignal.Wait();

                    var oldLength = pValue->DataLength;
                    pValue->DataLength = (uint)MinimumSize.GetValueOrDefault(pValue->DataLength);
                    if (pValue->DataLength > oldLength)
                        pValue->ExtraDataBytes -= (pValue->DataLength - oldLength);

                    tangle.BTree.UnlockValue(pValue, keyType);
                    tangle.BTree.UnlockNode(nodeRange);
                }
            }

            public override void Dispose () {
                DisposedSignal.Dispose();
                base.Dispose();
            }
        }

        private class GetMultipleThunk<TKey> : ThunkBase<T[]> {
            public readonly IEnumerable<TKey> Keys;
            public readonly TangleKeyConverter<TKey> KeyConverter;

            public GetMultipleThunk (IEnumerable<TKey> keys) {
                Keys = keys;
                KeyConverter = TangleKey.GetConverter<TKey>();
            }

            protected static int? GetCountFast (IEnumerable<TKey> keys) {
                TKey[] array = keys as TKey[];
                ICollection<TKey> collection = keys as ICollection<TKey>;

                if (array != null)
                    return array.Length;
                else if (collection != null)
                    return collection.Count;
                else
                    return null;
            }

            protected override void OnExecute (Tangle<T> tangle, out T[] result) {
                var keys = Keys;

                var count = GetCountFast(Keys);
                if (!count.HasValue) {
                    var array = Keys.ToArray();
                    keys = array;
                    count = array.Length;
                }

                var results = new T[count.Value];

                Parallel.ForEach(
                    keys, (rawKey, loopState, i) => {
                        T value;
                        if (tangle.InternalGet(KeyConverter(rawKey), out value))
                            results[i] = value;
                        else
                            results[i] = default(T);
                    }
                );

                result = results;
            }
        }

        private class JoinBarrierThunk : ThunkBase<NoneType> {
            public readonly ManualResetEventSlim ReadyForJoinSignal = new ManualResetEventSlim(false);
            public readonly ManualResetEventSlim JoinCompleteSignal = new ManualResetEventSlim(false);

            protected override void OnExecute (Tangle<T> tangle, out NoneType result) {
                ReadyForJoinSignal.Set();
                JoinCompleteSignal.Wait();
                result = default(NoneType);
            }

            public override void Dispose () {
                ReadyForJoinSignal.Dispose();
                JoinCompleteSignal.Dispose();
                base.Dispose();
            }
        }

        private class JoinThunk<TLeftKey, TRightKey, TRight, TOut> : ThunkBase<TOut[]> {
            public readonly Tangle<TRight>.JoinBarrierThunk RightBarrier;
            public readonly Tangle<TRight> Right;
            public readonly IEnumerable<TLeftKey> Keys;
            public readonly JoinKeySelector<TLeftKey, T, TRightKey> KeySelector;
            public readonly JoinValueSelector<TLeftKey, T, TRightKey, TRight, TOut> ValueSelector;
            public readonly TangleKeyConverter<TLeftKey> LeftKeyConverter;
            public readonly TangleKeyConverter<TRightKey> RightKeyConverter;

            public JoinThunk (
                Tangle<TRight>.JoinBarrierThunk rightBarrier,
                Tangle<TRight> right,
                IEnumerable<TLeftKey> keys,
                JoinKeySelector<TLeftKey, T, TRightKey> keySelector,
                JoinValueSelector<TLeftKey, T, TRightKey, TRight, TOut> valueSelector
            ) {
                RightBarrier = rightBarrier;
                Right = right;
                Keys = keys;
                KeySelector = keySelector;
                ValueSelector = valueSelector;
                LeftKeyConverter = TangleKey.GetConverter<TLeftKey>();
                RightKeyConverter = TangleKey.GetConverter<TRightKey>();
            }

            protected static int? GetCountFast (IEnumerable<TLeftKey> keys) {
                TLeftKey[] array = keys as TLeftKey[];
                ICollection<TLeftKey> collection = keys as ICollection<TLeftKey>;

                if (array != null)
                    return array.Length;
                else if (collection != null)
                    return collection.Count;
                else
                    return null;
            }

            protected override void OnExecute (Tangle<T> tangle, out TOut[] result) {
                var keys = Keys;

                var count = GetCountFast(Keys);
                if (!count.HasValue) {
                    var array = Keys.ToArray();
                    keys = array;
                    count = array.Length;
                }

                var results = new TOut[count.Value];

                if (RightBarrier != null)
                    RightBarrier.ReadyForJoinSignal.Wait();

                Parallel.ForEach(
                    keys, (leftKey, loopState, i) => {
                        T leftValue;
                        TRight rightValue;

                        if (!tangle.InternalGet(LeftKeyConverter(leftKey), out leftValue))
                            return;

                        var rightKey = KeySelector(leftKey, ref leftValue);
                        if (!Right.InternalGet(RightKeyConverter(rightKey), out rightValue))
                            return;

                        results[i] = ValueSelector(leftKey, ref leftValue, rightKey, ref rightValue);
                    }
                );

                result = results;

                if (RightBarrier != null)
                    RightBarrier.JoinCompleteSignal.Set();
            }
        }

        private class GetAllValuesThunk : ThunkBase<T[]> {
            protected override void OnExecute (Tangle<T> tangle, out T[] result) {
                var nodeCount = tangle.BTree.NodeCount;
                var count = tangle.Count;
                int position = 0;

                result = new T[count];

                for (int node = 0; node < nodeCount; node++)
                    position += tangle.BTree.GetNodeValues(node, tangle.Deserializer, result, position);

                if (position != count)
                    throw new InvalidDataException();
            }
        }

        private class GetAllKeysThunk : ThunkBase<TangleKey[]> {
            protected override void OnExecute (Tangle<T> tangle, out TangleKey[] result) {
                var nodeCount = tangle.BTree.NodeCount;
                var count = tangle.Count;
                int position = 0;

                result = new TangleKey[count];

                for (int node = 0; node < nodeCount; node++)
                    position += tangle.BTree.GetNodeKeys(node, result, position);

                if (position != count)
                    throw new InvalidDataException();
            }
        }

        private class FindThunk : ThunkBase<FindResult> {
            public readonly TangleKey Key;

            public FindThunk (TangleKey key) {
                Key = key;
            }

            protected override void OnExecute (Tangle<T> tangle, out FindResult result) {
                if (!tangle.InternalFind(Key, out result))
                    Fail(new KeyNotFoundException(Key));
            }
        }

        private class GetByIndexThunk : ThunkBase<T> {
            public readonly long NodeIndex;
            public readonly uint ValueIndex;

            public GetByIndexThunk (long nodeIndex, uint valueIndex) {
                NodeIndex = nodeIndex;
                ValueIndex = valueIndex;
            }

            protected override void OnExecute (Tangle<T> tangle, out T result) {
                tangle.InternalGetFoundValue(NodeIndex, ValueIndex, out result);
            }
        }

        private class SetByIndexThunk : SetThunkBase<NoneType> {
            public readonly long NodeIndex;
            public readonly uint ValueIndex;

            public SetByIndexThunk (long nodeIndex, uint valueIndex, ref T value) {
                NodeIndex = nodeIndex;
                ValueIndex = valueIndex;
                Value = value;
            }

            protected override void OnExecute (Tangle<T> tangle, out NoneType result) {
                tangle.InternalSetFoundValue(NodeIndex, ValueIndex, ref Value);
                result = NoneType.None;
            }
        }

        public class Barrier : ThunkBase<NoneType>, IBarrier {
            private readonly ManualResetEventSlim OpenSignal;
            private readonly Future<NoneType> ThunkSignal;

            internal Barrier (Tangle<T> tangle, bool opened) {
                OpenSignal = new ManualResetEventSlim(opened);
                ThunkSignal = tangle.QueueWorkItem(this);
            }

            public void Open () {
                if (OpenSignal.IsSet)
                    throw new InvalidOperationException("The barrier is already open.");

                OpenSignal.Set();
            }

            protected override void OnExecute (Tangle<T> tangle, out NoneType result) {
                ThunkSignal.Complete();
                OpenSignal.Wait();

                result = NoneType.None;
            }

            void ISchedulable.Schedule (TaskScheduler scheduler, IFuture future) {
                future.Bind(ThunkSignal);
            }

            new public IFuture Future {
                get {
                    return ThunkSignal;
                }
            }

            public override void Dispose () {
                OpenSignal.Set();
                ThunkSignal.Dispose();
                base.Dispose();
            }
        }
    }

    public partial class Index<TIndexKey, TValue> {
        private abstract class ThunkBase<U> : Tangle<TValue>.ThunkBase<U> {
            public readonly Index<TIndexKey, TValue> Index;

            public ThunkBase (Index<TIndexKey, TValue> index) {
                Index = index;
            }
        }

        private class CreateThunk : ThunkBase<Index<TIndexKey, TValue>> {
            public readonly string Name;
            public readonly Delegate Function;

            public CreateThunk (string name, Delegate function)
                : base (null) {
                Name = name;
                Function = function;
            }

            protected override void OnExecute (Tangle<TValue> tangle, out Index<TIndexKey, TValue> result) {
                result = new Index<TIndexKey, TValue>(tangle, Name, Function);
            }
        }

        private class FindOneThunk : ThunkBase<TangleKey> {
            public readonly TangleKey Key;

            public FindOneThunk (Index<TIndexKey, TValue> index, TangleKey key)
                : base(index) {
                    Key = key;
            }

            protected override unsafe void OnExecute (Tangle<TValue> tangle, out TangleKey result) {
                long nodeIndex;
                uint valueIndex;

                if (Index.BTree.FindKey(Key, false, out nodeIndex, out valueIndex)) {
                    HashSet<TangleKey> keys;
                    using (var range = Index.BTree.AccessValue(nodeIndex, valueIndex))
                        Index.BTree.ReadData((BTreeValue *)range.Pointer, Index<TIndexKey, TValue>.DeserializeKeys, out keys);

                    if (keys.Count == 0) {
                        Fail(new KeyNotFoundException(Key));
                        result = default(TangleKey);
                    } else
                        result = keys.First();
                } else {
                    result = default(TangleKey);
                    Fail(new KeyNotFoundException(Key));
                }
            }
        }

        private class GetOneThunk : ThunkBase<TValue> {
            public readonly TangleKey Key;

            public GetOneThunk (Index<TIndexKey, TValue> index, TangleKey key)
                : base(index) {
                Key = key;
            }

            protected override unsafe void OnExecute (Tangle<TValue> tangle, out TValue result) {
                long nodeIndex;
                uint valueIndex;

                if (Index.BTree.FindKey(Key, false, out nodeIndex, out valueIndex)) {
                    HashSet<TangleKey> keys;

                    using (var range = Index.BTree.AccessValue(nodeIndex, valueIndex))
                        Index.BTree.ReadData((BTreeValue*)range.Pointer, Index<TIndexKey, TValue>.DeserializeKeys, out keys);
                    
                    var firstKey = keys.First();
                    if (!tangle.InternalGet(firstKey, out result))
                        Fail(new KeyNotFoundException(firstKey));
                } else {
                    result = default(TValue);
                    Fail(new KeyNotFoundException(Key));
                }
            }
        }

        private class FindThunk : ThunkBase<TangleKey[]> {
            public readonly TangleKey Key;

            public FindThunk (Index<TIndexKey, TValue> index, TangleKey key)
                : base(index) {
                    Key = key;
            }

            protected override unsafe void OnExecute (Tangle<TValue> tangle, out TangleKey[] result) {
                long nodeIndex;
                uint valueIndex;

                result = null;
                if (Index.BTree.FindKey(Key, false, out nodeIndex, out valueIndex)) {
                    HashSet<TangleKey> keys;
                    using (var range = Index.BTree.AccessValue(nodeIndex, valueIndex))
                        Index.BTree.ReadData((BTreeValue *)range.Pointer, Index<TIndexKey, TValue>.DeserializeKeys, out keys);

                    result = keys.ToArray();
                } else {
                    Fail(new KeyNotFoundException(Key));
                }
            }
        }

        private class FindMultipleThunk : ThunkBase<TangleKey[]> {
            public readonly IEnumerable<TangleKey> Keys;

            public FindMultipleThunk (Index<TIndexKey, TValue> index, IEnumerable<TangleKey> keys)
                : base(index) {
                Keys = keys;
            }

            protected override unsafe void OnExecute (Tangle<TValue> tangle, out TangleKey[] result) {
                long nodeIndex;
                uint valueIndex;

                var resultSet = new HashSet<TangleKey>();

                foreach (var key in Keys) {
                    if (Index.BTree.FindKey(key, false, out nodeIndex, out valueIndex)) {
                        HashSet<TangleKey> keys;
                        using (var range = Index.BTree.AccessValue(nodeIndex, valueIndex))
                            Index.BTree.ReadData((BTreeValue*)range.Pointer, Index<TIndexKey, TValue>.DeserializeKeys, out keys);

                        resultSet.UnionWith(keys);
                    }
                }
                
                // Maybe we should throw for missing keys?

                result = resultSet.ToArray();
            }
        }

        private class GetThunk : ThunkBase<TValue[]> {
            public readonly TangleKey Key;

            public GetThunk (Index<TIndexKey, TValue> index, TangleKey key)
                : base(index) {
                Key = key;
            }

            protected override unsafe void OnExecute (Tangle<TValue> tangle, out TValue[] result) {
                long nodeIndex;
                uint valueIndex;

                result = null;
                if (Index.BTree.FindKey(Key, false, out nodeIndex, out valueIndex)) {
                    HashSet<TangleKey> keys;
                    using (var range = Index.BTree.AccessValue(nodeIndex, valueIndex))
                        Index.BTree.ReadData((BTreeValue*)range.Pointer, Index<TIndexKey, TValue>.DeserializeKeys, out keys);

                    result = new TValue[keys.Count];
                    int i = 0;
                    foreach (var key in keys) {
                        tangle.InternalGet(key, out result[i]);
                        i += 1;
                    }
                } else {
                    Fail(new KeyNotFoundException(Key));
                }
            }
        }

        private class GetMultipleThunk : ThunkBase<TValue[]> {
            public readonly IEnumerable<TangleKey> Keys;

            public GetMultipleThunk (Index<TIndexKey, TValue> index, IEnumerable<TangleKey> keys)
                : base(index) {
                Keys = keys;
            }

            protected override unsafe void OnExecute (Tangle<TValue> tangle, out TValue[] result) {
                long nodeIndex;
                uint valueIndex;

                var matchedKeys = new HashSet<TangleKey>();

                foreach (var key in Keys) {
                    if (Index.BTree.FindKey(key, false, out nodeIndex, out valueIndex)) {
                        HashSet<TangleKey> keys;
                        using (var range = Index.BTree.AccessValue(nodeIndex, valueIndex))
                            Index.BTree.ReadData((BTreeValue*)range.Pointer, Index<TIndexKey, TValue>.DeserializeKeys, out keys);

                        matchedKeys.UnionWith(keys);
                    }
                }

                // Maybe we should throw for missing keys or values?

                var resultArray = new TValue[matchedKeys.Count];
                Parallel.ForEach(matchedKeys, (key, loopState, i) => {
                    tangle.InternalGet(key, out resultArray[i]);
                });

                result = resultArray;
            }
        }

        private class GetAllKeysThunk : ThunkBase<TangleKey[]> {
            public GetAllKeysThunk (Index<TIndexKey, TValue> tangle)
                : base (tangle) {
            }

            protected override void OnExecute (Tangle<TValue> tangle, out TangleKey[] result) {
                var nodeCount = Index.BTree.NodeCount;
                var count = Index.BTree.ValueCount;
                int position = 0;

                result = new TangleKey[count];

                for (int node = 0; node < nodeCount; node++)
                    position += Index.BTree.GetNodeKeys(node, result, position);

                if (position != count)
                    throw new InvalidDataException();
            }
        }
    }
}
