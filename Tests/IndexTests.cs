using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using Squared.Task;

namespace Squared.Data.Mangler.Tests {
    [TestFixture]
    public class IndexTests : BasicTestFixture {
        public Tangle<string> Tangle;
        public Index<string, string> ByValue;

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
        }

        [TearDown]
        public override void TearDown () {
            Tangle.Dispose();
            base.TearDown();
        }

        [Test]
        public void IndexUpdatedWhenAddingNewValues () {
            ByValue = Scheduler.WaitFor(Tangle.CreateIndex("ByValue", (v) => v));

            var key = new TangleKey("hello");
            var value = "world";

            Scheduler.WaitFor(Tangle.Set(key, value));

            Assert.AreEqual(key, Scheduler.WaitFor(ByValue.FindOne(value)));
            Assert.AreEqual(value, Scheduler.WaitFor(ByValue.GetOne(value)));
        }

        [Test]
        public void IndexHandlesMultipleKeysForTheSameValue () {
            ByValue = Scheduler.WaitFor(Tangle.CreateIndex("ByValue", (v) => v));

            var key1 = new TangleKey("hello");
            var key2 = new TangleKey("greetings");
            var value = "world";

            Scheduler.WaitFor(Tangle.Set(key1, value));
            Scheduler.WaitFor(Tangle.Set(key2, value));

            Assert.AreEqual(
                new TangleKey[] { key1, key2 }, 
                Scheduler.WaitFor(ByValue.Find(value))
            );
            Assert.AreEqual(
                new string[] { value, value }, 
                Scheduler.WaitFor(ByValue.Get(value))
            );
        }

        [Test]
        public void IndexUpdatedWhenValueChanged () {
            ByValue = Scheduler.WaitFor(Tangle.CreateIndex("ByValue", (v) => v));

            var key = new TangleKey("hello");
            var value1 = "world";
            var value2 = "place";

            Scheduler.WaitFor(Tangle.Set(key, value1));

            Assert.AreEqual(key, Scheduler.WaitFor(ByValue.FindOne(value1)));

            Scheduler.WaitFor(Tangle.Set(key, value2));

            try {
                Scheduler.WaitFor(ByValue.FindOne(value1));
                Assert.Fail("Expected to throw");
            } catch (FutureException fe) {
                Assert.IsInstanceOf<KeyNotFoundException>(fe.InnerException);
            }

            Assert.AreEqual(key, Scheduler.WaitFor(ByValue.FindOne(value2)));
        }

        [Test]
        public void CanAddIndexToTangleWithExistingValues () {
            var key1 = new TangleKey("hello");
            var value1 = "world";
            var key2 = new TangleKey("greetings");
            var value2 = "place";

            Scheduler.WaitFor(Tangle.Set(key1, value1));
            Scheduler.WaitFor(Tangle.Set(key2, value2));

            ByValue = Scheduler.WaitFor(Tangle.CreateIndex("ByValue", (v) => v));

            Assert.AreEqual(key1, Scheduler.WaitFor(ByValue.FindOne(value1)));
            Assert.AreEqual(key2, Scheduler.WaitFor(ByValue.FindOne(value2)));
        }
    }
}
