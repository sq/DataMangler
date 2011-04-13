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
