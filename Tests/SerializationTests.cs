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
using NUnit.Framework;
using System.IO;
using Squared.Task;

namespace Squared.Data.Mangler.Tests {
    public struct SpecialType {
        public readonly TangleKey Key;
        public readonly UInt32 Value;

        public SpecialType (TangleKey key, UInt32 value) {
            Key = key;
            Value = value;
        }

        [TangleDeserializer]
        static void Deserialize (ref DeserializationContext context, out SpecialType output) {
            var br = new BinaryReader(context.Stream);
            output = new SpecialType(context.Key, br.ReadUInt32());
        }

        [TangleSerializer]
        static void Serialize (ref SerializationContext context, ref SpecialType input) {
            if (!context.Key.Equals(input.Key))
                throw new InvalidDataException();

            var bw = new BinaryWriter(context.Stream);
            bw.Write(input.Value);
            bw.Flush();
        }
    }

    [TestFixture]
    public class SerializationTests : BasicTestFixture {
        public Tangle<SpecialType> Tangle;

        [SetUp]
        public override void SetUp () {
            base.SetUp();

            Tangle = new Tangle<SpecialType>(
                Scheduler, Storage,
                ownsStorage: true
            );
        }

        [TearDown]
        public override void TearDown () {
            Tangle.Dispose();
            base.TearDown();
        }

        [Test]
        public void UsesStaticSerializerAndDeserializerMethodsAutomatically () {
            var key = new TangleKey("hello");
            Scheduler.WaitFor(Tangle.Set(key, new SpecialType(key, 4)));
            var result = Scheduler.WaitFor(Tangle.Get("hello"));
            Assert.AreEqual(4, result.Value);
        }

        [Test]
        public void SerializerAndDeserializerHaveAccessToKey () {
            var key = new TangleKey("hello");

            // This will fail because the specified keys don't match, and that lets us know
            //  that the serializer had access to the key. Kind of a hack.
            try {
                Scheduler.WaitFor(Tangle.Set("world", new SpecialType(key, 4)));
            } catch (FutureException fe) {
                Assert.IsInstanceOf<SerializerThrewException>(fe.InnerException);
                Assert.IsInstanceOf<InvalidDataException>(fe.InnerException.InnerException);
            }

            // As a side effect, this also tests the Tangle's ability to recover from
            //  a failed serialization. If an exception from the Serializer were to bubble
            //  up, the BTree would be left in an invalid state and this set would fail.
            Scheduler.WaitFor(Tangle.Set(key, new SpecialType(key, 4)));

            var result = Scheduler.WaitFor(Tangle.Get("hello"));
            Assert.IsTrue(key.Equals(result.Key));
        }
    }

    [TestFixture]
    public class PropertySerializerTests : BasicTestFixture {
        public class ClassWithProperties {
            public int A;
            public int B {
                get;
                set;
            }
            public string C;
        }

        public Tangle<object> Tangle;
        public TanglePropertySerializer Serializer;

        [SetUp]
        public override void SetUp () {
            base.SetUp();
            Tangle = new Tangle<object>(Scheduler, Storage);
            Serializer = new TanglePropertySerializer(Tangle);
        }

        [TearDown]
        public override void TearDown () {
            Tangle.Dispose();
            base.TearDown();
        }

        [Test]
        public void TestSerializesProperties () {
            var instance = new ClassWithProperties {
                A = 1,
                B = 2,
                C = "foo"
            };

            Serializer.Bind(() => instance.A);
            Serializer.Bind(() => instance.B);
            Serializer.Bind(() => instance.C);

            Scheduler.WaitFor(Serializer.Save());

            Assert.AreEqual(1, (int)Scheduler.WaitFor(Tangle.Get("A")));
            Assert.AreEqual(2, (int)Scheduler.WaitFor(Tangle.Get("B")));
            Assert.AreEqual("foo", (string)Scheduler.WaitFor(Tangle.Get("C")));
        }
    }
}
