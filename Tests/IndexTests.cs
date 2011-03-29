using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;

namespace Squared.Data.Mangler.Tests {
    /*
    [TestFixture]
    public class IndexTests : BasicTestFixture {
        public Tangle<string> Tangle;
        public Index<string> ByValue;

        [SetUp]
        public unsafe override void SetUp () {
            base.SetUp();

            var serializer = new Squared.Data.Mangler.Serialization.StringSerializer(
                Encoding.UTF8
            );

            Tangle = new Tangle<string>(
                Scheduler, Storage,
                serializer: serializer.Serialize,
                deserializer: serializer.Deserialize,
                ownsStorage: true
            );

            ByValue = Tangle.CreateIndex((value) => value);
        }

        [TearDown]
        public override void TearDown () {
            Tangle.Dispose();
            base.TearDown();
        }

        [Test]
        public void IndexUpdatedWhenAddingNewValues () {
            Scheduler.WaitFor(Tangle.Set("hello", "world"));

            Assert.AreEqual(
        }
    }
     */
}
