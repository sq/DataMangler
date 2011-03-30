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
    public interface ITangle : IDisposable {
        /// <summary>
        /// Creates a barrier and inserts it into the tangle's work queue. 
        /// The barrier is signaled when reached and prevents work items following it from being executed as long as it remains closed.
        /// </summary>
        /// <param name="createOpened">If true, the barrier is created opened.</param>
        /// <returns>The created barrier.</returns>
        IBarrier CreateBarrier (bool createOpened);

        /// <summary>
        /// Retrieves a value from the tangle, looking it up via the specified key.
        /// </summary>
        /// <param name="key">The key of the value to retrieve.</param>
        /// <returns>The retrieved value.</returns>
        IFuture Get (TangleKey key);

        /// <summary>
        /// Retrieves all the values stored within the tangle, in no particular order.
        /// </summary>
        /// <returns>The values stored within the tangle, as an array of type T[].</returns>
        IFuture GetAllValues ();

        /// <summary>
        /// Retrieves the keys of all the values stored within the tangle, in no particular order.
        /// </summary>
        /// <returns>The keys of the tangle's values.</returns>
        Future<TangleKey[]> GetAllKeys ();

        /// <summary>
        /// The number of values stored within the tangle.
        /// </summary>
        long Count {
            get;
        }
    }

    public interface IBarrier : ISchedulable, IDisposable {
        /// <summary>
        /// Opens the barrier, allowing work items later in the queue to be executed.
        /// </summary>
        void Open ();

        /// <summary>
        /// Returns a future that becomes completed once the barrier has been reached, regardless of whether the barrier is open or closed.
        /// </summary>
        IFuture Future {
            get;
        }
    }

    public interface IBatch {
        Future<int> Execute ();
    }

    /// <summary>
    /// Called to update a value within a tangle.
    /// </summary>
    /// <param name="oldValue">The current value of the item.</param>
    /// <returns>The new value of the item.</returns>
    public delegate T UpdateCallback<T> (T oldValue);

    /// <summary>
    /// Called to update a value within a tangle.
    /// </summary>
    /// <param name="oldValue">The current value of the item.</param>
    /// <param name="newValue">The new value of the item. By default, this is the value provided when calling AddOrUpdate.</param>
    /// <returns>True to update the item's value, false to abort.</returns>
    public delegate bool DecisionUpdateCallback<T> (ref T oldValue, ref T newValue);

    /// <summary>
    /// Called to generate the right side key for a join.
    /// </summary>
    /// <typeparam name="TLeftKey">The type of the left side key(s).</typeparam>
    /// <typeparam name="TLeft">The type of the left side value(s).</typeparam>
    /// <typeparam name="TRightKey">The type of the right side key(s).</typeparam>
    /// <param name="leftKey">The left side key.</param>
    /// <param name="leftValue">The left side value.</param>
    /// <returns>The right side key.</returns>
    public delegate TRightKey JoinKeySelector<TLeftKey, TLeft, TRightKey> 
        (TLeftKey leftKey, ref TLeft leftValue);

    /// <summary>
    /// Called to generate the result of a join.
    /// </summary>
    /// <typeparam name="TLeftKey">The type of the left side key(s).</typeparam>
    /// <typeparam name="TLeft">The type of the left side value(s).</typeparam>
    /// <typeparam name="TRightKey">The type of the right side key(s).</typeparam>
    /// <typeparam name="TRight">The type of the right side value(s).</typeparam>
    /// <typeparam name="TOut">The type of the result value(s).</typeparam>
    /// <param name="leftKey">The left side key.</param>
    /// <param name="leftValue">The left side value.</param>
    /// <param name="rightKey">The right side key.</param>
    /// <param name="rightValue">The right side value.</param>
    /// <returns>The join result.</returns>
    public delegate TOut JoinValueSelector<TLeftKey, TLeft, TRightKey, TRight, out TOut> 
        (TLeftKey leftKey, ref TLeft leftValue, TRightKey rightKey, ref TRight rightValue);

    public delegate TIndexKey IndexFunc<TIndexKey, TValue> (TValue value);

    public delegate TangleKey TangleKeyConverter<TValue> (TValue value);
}

namespace Squared.Data.Mangler.Internal {
    public interface IWorkItem<T> : IDisposable {
        void Execute (Tangle<T> tangle);
    }

    public interface IWorkItemWithFuture<T, U> : IWorkItem<T> {
        Future<U> Future {
            get;
        }
    }

    internal interface IReplaceCallback<T> {
        bool ShouldReplace (Tangle<T> tangle, ref BTreeValue btreeValue, ushort keyType, ref T newValue);
    }
}
