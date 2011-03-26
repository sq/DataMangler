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
        IBarrier CreateBarrier (bool createOpened);

        IEnumerable<TangleKey> Keys {
            get;
        }
        long Count {
            get;
        }
    }

    public interface IBarrier : ISchedulable, IDisposable {
        void Open ();

        IFuture Future {
            get;
        }
    }
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
}
