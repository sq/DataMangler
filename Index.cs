using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Squared.Data.Mangler {
    public class Index<TKey, TValue> {
        public readonly Tangle<TValue> Tangle;
        public readonly IndexFunc<TKey, TValue> IndexFunction;

        private readonly TangleKeyConverter<TKey> KeyConverter;

        public Index (Tangle<TValue> tangle, IndexFunc<TKey, TValue> function) {
            Tangle = tangle;
            IndexFunction = function;
            KeyConverter = TangleKey.GetConverter<TKey>();
        }
    }
}
