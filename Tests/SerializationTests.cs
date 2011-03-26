using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using System.IO;

namespace Squared.Data.Mangler.Tests {
    public struct SpecialType {
        public readonly UInt32 Value;

        public SpecialType (UInt32 value) {
            Value = value;
        }

        [TangleDeserializer]
        static void Deserialize (ref DeserializationContext context, out SpecialType output) {
            var br = new BinaryReader(context.Stream);
            output = new SpecialType(br.ReadUInt32());
        }

        [TangleSerializer]
        static void Serialize (ref SerializationContext context, ref SpecialType input) {
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
            Scheduler.WaitFor(Tangle.Set("hello", new SpecialType(4)));
            Assert.AreEqual(4, Scheduler.WaitFor(Tangle.Get("hello")).Value);
        }
    }
}
