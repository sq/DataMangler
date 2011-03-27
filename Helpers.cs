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
using System.Text;
using Squared.Task;

namespace Squared.Data.Mangler {
    internal struct BatchItem<T> {
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

    /// <summary>
    /// A Batch allows you to apply multiple modifications to a Tangle as a single work item.
    /// To use it, create an instance of the appropriate batch type, add modifications to it using its methods, and then Execute it on the Tangle.
    /// Using a batch to modify a Tangle is faster than calling the Tangle's methods individually.
    /// </summary>
    /// <typeparam name="T">The type of the tangle's items.</typeparam>
    public class Batch<T> {
        public readonly int Capacity;
        internal readonly BatchItem<T>[] Buffer;
        private int _Count;

        /// <summary>
        /// Creates a batch.
        /// </summary>
        /// <param name="capacity">The maximum number of modifications that the batch can contain.</param>
        public Batch (int capacity) {
            Capacity = capacity;
            Buffer = new BatchItem<T>[capacity];
        }

        /// <summary>
        /// The number of modifications currently contained by the batch.
        /// </summary>
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

        /// <summary>
        /// Adds the batch to the Tangle's work queue.
        /// </summary>
        /// <param name="tangle">The tangle to modify.</param>
        /// <returns>A future that becomes completed once all the work items in the batch have completed.</returns>
        public Future<int> Execute (Tangle<T> tangle) {
            return tangle.QueueWorkItem(new Tangle<T>.BatchThunk(this));
        }
    }

    /// <summary>
    /// Represents a collection of barriers in one or more tangles.
    /// </summary>
    public class BarrierCollection : IBarrier {
        private readonly List<IBarrier> Barriers = new List<IBarrier>();
        private readonly IFuture Composite;

        /// <summary>
        /// Inserts barriers into one or more tangles and creates a barrier collection out of them.
        /// </summary>
        /// <param name="waitForAll">If true, the barrier collection will not become signaled until all the barriers within it are signaled. Otherwise, only one barrier within the collection must become signaled before the collection is signaled.</param>
        /// <param name="tangles">The list of tangles to create barriers in.</param>
        public BarrierCollection (bool waitForAll, IEnumerable<ITangle> tangles) {
            foreach (var tangle in tangles)
                Barriers.Add(tangle.CreateBarrier(false));

            var futures = from barrier in Barriers select barrier.Future;
            if (waitForAll)
                Composite = Squared.Task.Future.WaitForAll(futures);
            else
                Composite = Squared.Task.Future.WaitForFirst(futures);
        }

        /// <summary>
        /// Inserts barriers into one or more tangles and creates a barrier collection out of them.
        /// </summary>
        /// <param name="waitForAll">If true, the barrier collection will not become signaled until all the barriers within it are signaled. Otherwise, only one barrier within the collection must become signaled before the collection is signaled.</param>
        /// <param name="tangles">The list of tangles to create barriers in.</param>
        public BarrierCollection (bool waitForAll, params ITangle[] tangles)
            : this(waitForAll, (IEnumerable<ITangle>)tangles) {
        }

        public void Dispose () {
            foreach (var barrier in Barriers)
                barrier.Dispose();

            Barriers.Clear();
            Composite.Dispose();
        }

        /// <summary>
        /// Opens all the barriers within the collection.
        /// </summary>
        public void Open () {
            foreach (var barrier in Barriers)
                barrier.Open();
        }

        public IFuture Future {
            get {
                return Composite;
            }
        }

        void ISchedulable.Schedule (TaskScheduler scheduler, IFuture future) {
            future.Bind(Composite);
        }
    }
}
