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

            bool IReplaceCallback<T>.ShouldReplace (Tangle<T> tangle, ref IndexEntry indexEntry, ref T newValue) {
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

            bool IReplaceCallback<T>.ShouldReplace (Tangle<T> tangle, ref IndexEntry indexEntry, ref T newValue) {
                if (Callback != null) {
                    T oldValue;
                    tangle.ReadData(ref indexEntry, out oldValue);
                    newValue = Callback(oldValue);
                    return true;
                } else if (DecisionCallback != null) {
                    tangle.ReadData(ref indexEntry, out newValue);
                    return DecisionCallback(ref newValue);
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

            bool IReplaceCallback<T>.ShouldReplace (Tangle<T> tangle, ref IndexEntry indexEntry, ref T newValue) {
                if (Callback != null) {
                    T oldValue;
                    tangle.ReadData(ref indexEntry, out oldValue);
                    newValue = Callback(oldValue);
                    return true;
                } else {
                    tangle.ReadData(ref indexEntry, out newValue);
                    return DecisionCallback(ref newValue);
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

        private class GetMultipleThunk<TKey> : ThunkBase<KeyValuePair<TKey, T>[]> {
            public readonly IEnumerable<TKey> Keys;
            public readonly Func<TKey, TangleKey> KeyConverter;

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

            protected override void OnExecute (Tangle<T> tangle, out KeyValuePair<TKey, T>[] result) {
                var keys = Keys;

                var count = GetCountFast(Keys);
                if (!count.HasValue) {
                    var array = Keys.ToArray();
                    keys = array;
                    count = array.Length;
                }

                var results = new KeyValuePair<TKey, T>[count.Value];

                Parallel.ForEach(
                    keys, (rawKey, loopState, i) => {
                        T value;
                        if (tangle.InternalGet(KeyConverter(rawKey), out value))
                            results[i] = new KeyValuePair<TKey, T>(rawKey, value);
                        else
                            results[i] = new KeyValuePair<TKey, T>(rawKey, default(T));
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
        }

        private class JoinThunk<TLeftKey, TRightKey, TRight, TOut> : ThunkBase<TOut[]> {
            public readonly Tangle<TRight>.JoinBarrierThunk RightBarrier;
            public readonly Tangle<TRight> Right;
            public readonly IEnumerable<TLeftKey> Keys;
            public readonly JoinKeySelector<TLeftKey, T, TRightKey> KeySelector;
            public readonly JoinValueSelector<TLeftKey, T, TRightKey, TRight, TOut> ValueSelector;
            public readonly Func<TLeftKey, TangleKey> LeftKeyConverter;
            public readonly Func<TRightKey, TangleKey> RightKeyConverter;

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

                RightBarrier.JoinCompleteSignal.Set();
            }
        }

        private class GetAllValuesThunk : ThunkBase<T[]> {
            protected override void OnExecute (Tangle<T> tangle, out T[] result) {
                var nodeCount = tangle.BTreeNodeCount;
                var count = tangle.Count;
                int position = 0;

                result = new T[count];

                for (int node = 0; node < nodeCount; node++)
                    position += tangle.InternalGetNodeValues(node, result, position);

                if (position != count)
                    throw new InvalidDataException();
            }
        }

        private class GetAllKeysThunk : ThunkBase<TangleKey[]> {
            protected override void OnExecute (Tangle<T> tangle, out TangleKey[] result) {
                var nodeCount = tangle.BTreeNodeCount;
                var count = tangle.Count;
                int position = 0;

                result = new TangleKey[count];

                for (int node = 0; node < nodeCount; node++)
                    position += tangle.InternalGetNodeKeys(node, result, position);

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
}
