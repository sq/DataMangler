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
using Squared.Task.Data;
using Squared.Util.Bind;

namespace Squared.Data.Mangler {
    public class TanglePropertySerializer : PropertySerializerBase {
        public readonly Tangle<object> Tangle;

        public TanglePropertySerializer (
            Tangle<object> tangle
        ) : this (tangle, GetDefaultMemberName) {
        }

        public TanglePropertySerializer (
            Tangle<object> tangle, Func<IBoundMember, string> getMemberName
        ) : base(getMemberName) {
            Tangle = tangle;
        }

        protected override IEnumerator<object> SaveBinding<T> (string name, BoundMember<T> member) {
            yield return Tangle.Set(name, member.Value);
        }

        protected override IEnumerator<object> LoadBinding<T> (string name, BoundMember<T> member) {
            var fValue = Tangle.Get(name);
            yield return fValue;

            if (!fValue.Failed)
                member.Value = (T)fValue.Result;
        }
    }
}
