using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Squared.Task;

namespace Squared.Data.Mangler {
    public struct BatchItem<T> {
        public readonly bool AllowReplacement;
        public readonly UpdateCallback<T> Callback;
        public readonly DecisionUpdateCallback<T> DecisionCallback;
        public TangleKey Key;
        public T Value;

        public BatchItem (TangleKey key, ref T value, bool allowReplacement) {
            Key = key;
            Value = value;
            AllowReplacement = allowReplacement;
            Callback = null;
            DecisionCallback = null;
        }

        public BatchItem (TangleKey key, ref T value, UpdateCallback<T> callback) {
            Key = key;
            Value = value;
            AllowReplacement = false;
            Callback = callback;
            DecisionCallback = null;
        }

        public BatchItem (TangleKey key, ref T value, DecisionUpdateCallback<T> callback) {
            Key = key;
            Value = value;
            AllowReplacement = false;
            Callback = null;
            DecisionCallback = callback;
        }
    }

    public class Batch<T> {
        public readonly int Capacity;
        internal readonly BatchItem<T>[] Buffer;
        private int _Count;

        public Batch (int capacity) {
            Capacity = capacity;
            Buffer = new BatchItem<T>[capacity];
        }

        public int Count {
            get {
                return _Count;
            }
        }

        public void Add (TangleKey key, T value) {
            Add(key, ref value);
        }

        public void Add (TangleKey key, ref T value) {
            if (_Count >= Capacity)
                throw new IndexOutOfRangeException();

            Buffer[_Count++] = new BatchItem<T>(key, ref value, false);
        }

        public void Set (TangleKey key, T value) {
            Set(key, ref value);
        }

        public void Set (TangleKey key, ref T value) {
            if (_Count >= Capacity)
                throw new IndexOutOfRangeException();

            Buffer[_Count++] = new BatchItem<T>(key, ref value, true);
        }

        public void AddOrUpdate (TangleKey key, T value, UpdateCallback<T> updateCallback) {
            AddOrUpdate(key, ref value, updateCallback);
        }

        public void AddOrUpdate (TangleKey key, ref T value, UpdateCallback<T> updateCallback) {
            if (_Count >= Capacity)
                throw new IndexOutOfRangeException();

            Buffer[_Count++] = new BatchItem<T>(key, ref value, updateCallback);
        }

        public void AddOrUpdate (TangleKey key, T value, DecisionUpdateCallback<T> updateCallback) {
            AddOrUpdate(key, ref value, updateCallback);
        }

        public void AddOrUpdate (TangleKey key, ref T value, DecisionUpdateCallback<T> updateCallback) {
            if (_Count >= Capacity)
                throw new IndexOutOfRangeException();

            Buffer[_Count++] = new BatchItem<T>(key, ref value, updateCallback);
        }

        public Future<int> Execute (Tangle<T> tangle) {
            return tangle.QueueWorkItem(new Tangle<T>.BatchThunk(this));
        }
    }

    public class BarrierCollection : IBarrier, IDisposable {
        private readonly List<IBarrier> Barriers = new List<IBarrier>();
        private readonly IFuture Composite;

        public BarrierCollection (bool waitForAll, IEnumerable<ITangle> tangles) {
            foreach (var tangle in tangles)
                Barriers.Add(tangle.CreateBarrier(false));

            var futures = from barrier in Barriers select barrier.Future;
            if (waitForAll)
                Composite = Squared.Task.Future.WaitForAll(futures);
            else
                Composite = Squared.Task.Future.WaitForFirst(futures);
        }

        public BarrierCollection (bool waitForAll, params ITangle[] tangles)
            : this(waitForAll, (IEnumerable<ITangle>)tangles) {
        }

        public void Dispose () {
            foreach (var barrier in Barriers)
                barrier.Dispose();

            Barriers.Clear();
            Composite.Dispose();
        }

        public void Open () {
            foreach (var barrier in Barriers)
                barrier.Open();
        }

        public IFuture Future {
            get {
                return Composite;
            }
        }

        public void Schedule (TaskScheduler scheduler, IFuture future) {
            future.Bind(Composite);
        }
    }
}
