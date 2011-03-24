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
using Squared.Task;
using System.IO;
using Squared.Util;

namespace Squared.Data.Mangler.Tests {
    public class BasicTestFixture {
        public string TestFile;
        public StreamSource Storage;
        public TaskScheduler Scheduler;

        [SetUp]
        public virtual void SetUp () {
            Scheduler = new TaskScheduler();

            TestFile = Path.GetTempFileName();
            Storage = new AlternateStreamSource(TestFile);
        }

        [TearDown]
        public virtual void TearDown () {
            Scheduler.Dispose();
            File.Delete(TestFile);
        }
    }

    [TestFixture]
    public class BasicTests : BasicTestFixture {
        public Tangle<int> Tangle;

        [SetUp]
        public override void SetUp () {
            base.SetUp();

            Tangle = new Tangle<int>(
                Scheduler, Storage, 
                serializer: (ref int i, Stream o) => 
                    o.Write(BitConverter.GetBytes(i), 0, 4),
                deserializer: (Stream i, out int o) => {
                    var bytes = new byte[4];
                    i.Read(bytes, 0, bytes.Length);
                    o = BitConverter.ToInt32(bytes, 0);
                },
                ownsStorage: true
            );
        }

        [TearDown]
        public override void TearDown () {
            // Tangle.ExportStreams(@"C:\dm_streams\");
            Tangle.Dispose();
            base.TearDown();
        }

        [Test]
        public void CanGetValueByNameAfterSettingIt () {
            Scheduler.WaitFor(Tangle.Set("hello", 1));
            Assert.AreEqual(1, Scheduler.WaitFor(Tangle.Get("hello")));
        }

        [Test]
        public void GetThrowsIfKeyIsNotFound () {
            try {
                Scheduler.WaitFor(Tangle.Get("missing"));
                Assert.Fail("Should have thrown");
            } catch (FutureException fe) {
                Assert.IsInstanceOf<KeyNotFoundException>(fe.InnerException);
            }
        }

        [Test]
        public void NumericKeysWork () {
            var key = new TangleKey(1234);
            Scheduler.WaitFor(Tangle.Set(key, 1));
            Assert.AreEqual(1, Scheduler.WaitFor(Tangle.Get(key)));
        }

        [Test]
        public void CanOverwriteExistingValueBySettingItAgain () {
            Scheduler.WaitFor(Tangle.Set("hello", 1));
            Scheduler.WaitFor(Tangle.Set("hello", 3));
            Assert.AreEqual(3, Scheduler.WaitFor(Tangle.Get("hello")));
        }

        [Test]
        public void AddReturnsFalseInsteadOfOverwriting () {
            Assert.AreEqual(true, Scheduler.WaitFor(Tangle.Add("hello", 1)));
            Assert.AreEqual(false, Scheduler.WaitFor(Tangle.Add("hello", 3)));
            Assert.AreEqual(1, Scheduler.WaitFor(Tangle.Get("hello")));
        }

        [Test]
        public void FindReturnsReferenceThatCanBeUsedToFetchValue () {
            Scheduler.WaitFor(Tangle.Set("a", 1));
            Scheduler.WaitFor(Tangle.Set("b", 2));

            var itemRef = Scheduler.WaitFor(Tangle.Find("a"));
            Assert.AreEqual("a", itemRef.Key.ToString());
            Assert.AreEqual(1, Scheduler.WaitFor(itemRef.GetValue()));
        }

        [Test]
        public void FindReturnsReferenceThatCanBeUsedToReplaceValue () {
            Scheduler.WaitFor(Tangle.Set("a", 1));
            Scheduler.WaitFor(Tangle.Set("b", 2));

            var itemRef = Scheduler.WaitFor(Tangle.Find("a"));
            Scheduler.WaitFor(itemRef.SetValue(3));
        }

        [Test]
        public void FindThrowsIfKeyIsNotFound () {
            try {
                Scheduler.WaitFor(Tangle.Find("missing"));
                Assert.Fail("Should have thrown");
            } catch (FutureException fe) {
                Assert.IsInstanceOf<KeyNotFoundException>(fe.InnerException);
            }
        }

        [Test]
        public void AddOrUpdateInvokesCallbackWhenKeyIsFound () {
            Scheduler.WaitFor(Tangle.Add("a", 1));
            Scheduler.WaitFor(Tangle.AddOrUpdate("a", 999, (oldValue) => oldValue + 1));
            Scheduler.WaitFor(Tangle.AddOrUpdate("b", 128, (oldValue) => oldValue + 1));

            Assert.AreEqual(2, Scheduler.WaitFor(Tangle.Get("a")));
            Assert.AreEqual(128, Scheduler.WaitFor(Tangle.Get("b")));
        }

        [Test]
        public void AddOrUpdateCallbackCanAbortUpdate () {
            Scheduler.WaitFor(Tangle.Add("a", 1));
            Scheduler.WaitFor(Tangle.AddOrUpdate("a", 999, (ref int value) => false));

            Assert.AreEqual(1, Scheduler.WaitFor(Tangle.Get("a")));
        }

        [Test]
        public void AddOrUpdateCallbackCanMutateValue () {
            Scheduler.WaitFor(Tangle.Add("a", 1));
            Scheduler.WaitFor(Tangle.AddOrUpdate("a", 999, (ref int value) => {
                value += 1;
                return true;
            }));

            Assert.AreEqual(2, Scheduler.WaitFor(Tangle.Get("a")));
        }

        [Test]
        public void InsertInSequentialOrder () {
            Scheduler.WaitFor(Tangle.Set("aa", 4));
            Scheduler.WaitFor(Tangle.Set("ea", 3));
            Scheduler.WaitFor(Tangle.Set("qa", 2));
            Scheduler.WaitFor(Tangle.Set("za", 1));

            Assert.AreEqual(
                new object[] { "aa", "ea", "qa", "za" }, (from k in Tangle.Keys select k.Value).ToArray()
            );
        }

        [Test]
        public void InsertInReverseOrder () {
            Scheduler.WaitFor(Tangle.Set("za", 4));
            Scheduler.WaitFor(Tangle.Set("qa", 3));
            Scheduler.WaitFor(Tangle.Set("ea", 2));
            Scheduler.WaitFor(Tangle.Set("aa", 1));

            Assert.AreEqual(
                new object[] { "aa", "ea", "qa", "za" }, (from k in Tangle.Keys select k.Value).ToArray()
            );
        }

        protected IEnumerator<object> WriteLotsOfValues (Tangle<int> tangle, int numIterations, int direction) {
            if (direction > 0)
                for (int i = 0; i < numIterations; i++) {
                    yield return tangle.Set(i, i);
                }
            else
                for (int i = numIterations - 1; i >= 0; i--) {
                    yield return tangle.Set(i, i);
                }
        }

        [Test]
        public void CanWriteLotsOfValuesSequentially () {
            const int numValues = 50000;

            long startTime = Time.Ticks;
            Scheduler.WaitFor(WriteLotsOfValues(Tangle, numValues, 1));
            decimal elapsedSeconds = (decimal)(Time.Ticks - startTime) / Time.SecondInTicks;
            Console.WriteLine(
                "Wrote {0} values in ~{1:00.000} second(s) at ~{2:00000.00} values/sec.",
                numValues, elapsedSeconds, numValues / elapsedSeconds
            );

            startTime = Time.Ticks;
            Scheduler.WaitFor(CheckLotsOfValues(Tangle, numValues));
            elapsedSeconds = (decimal)(Time.Ticks - startTime) / Time.SecondInTicks;
            Console.WriteLine(
                "Read {0} values in ~{1:00.000} second(s) at ~{2:00000.00} values/sec.",
                numValues, elapsedSeconds, numValues / elapsedSeconds
            );
        }

        [Test]
        public void CanWriteLotsOfValuesInReverse () {
            const int numValues = 500000;

            long startTime = Time.Ticks;
            Scheduler.WaitFor(WriteLotsOfValues(Tangle, numValues, -1));
            decimal elapsedSeconds = (decimal)(Time.Ticks - startTime) / Time.SecondInTicks;
            Console.WriteLine(
                "Wrote {0} values in ~{1:00.000} second(s) at ~{2:00000.00} values/sec.",
                numValues, elapsedSeconds, numValues / elapsedSeconds
            );

            startTime = Time.Ticks;
            Scheduler.WaitFor(CheckLotsOfValues(Tangle, numValues));
            elapsedSeconds = (decimal)(Time.Ticks - startTime) / Time.SecondInTicks;
            Console.WriteLine(
                "Read {0} values in ~{1:00.000} second(s) at ~{2:00000.00} values/sec.",
                numValues, elapsedSeconds, numValues / elapsedSeconds
            );
        }

        protected IEnumerator<object> WriteLotsOfValuesInBatch (Tangle<int> tangle, int numIterations, int direction) {
            int batchSize = 256;
            Tangle<int>.SetBatch batch = null;

            if (direction > 0)
                for (int i = 0; i < numIterations; i++) {
                    if (batch == null)
                        batch = new Tangle<int>.SetBatch(batchSize);

                    batch.Add(i, i);

                    if (batch.Count == batchSize) {
                        yield return batch.Execute(Tangle);
                        batch = null;
                    }
                }
            else
                for (int i = numIterations - 1; i >= 0; i--) {
                    if (batch == null)
                        batch = new Tangle<int>.SetBatch(batchSize);

                    batch.Add(i, i);

                    if (batch.Count == batchSize) {
                        yield return batch.Execute(Tangle);
                        batch = null;
                    }
                }

            if (batch != null)
                yield return batch.Execute(Tangle);
        }

        [Test]
        public void BatchValuesInReverse () {
            const int numValues = 500000;

            long startTime = Time.Ticks;
            Scheduler.WaitFor(WriteLotsOfValuesInBatch(Tangle, numValues, -1));
            decimal elapsedSeconds = (decimal)(Time.Ticks - startTime) / Time.SecondInTicks;
            Console.WriteLine(
                "Wrote {0} values in ~{1:00.000} second(s) at ~{2:00000.00} values/sec.",
                numValues, elapsedSeconds, numValues / elapsedSeconds
            );

            startTime = Time.Ticks;
            Scheduler.WaitFor(CheckLotsOfValues(Tangle, numValues));
            elapsedSeconds = (decimal)(Time.Ticks - startTime) / Time.SecondInTicks;
            Console.WriteLine(
                "Read {0} values in ~{1:00.000} second(s) at ~{2:00000.00} values/sec.",
                numValues, elapsedSeconds, numValues / elapsedSeconds
            );
        }

        protected IEnumerator<object> CheckLotsOfValues (Tangle<int> tangle, int numIterations) {
            for (int i = 0; i < numIterations; i++) {
                var f = tangle.Get(i);
                yield return f;
                Assert.AreEqual(i, f.Result);
            }
        }
    }

    [TestFixture]
    public class StringTests : BasicTestFixture {
        public Tangle<string> Tangle;

        [SetUp]
        public override void SetUp () {
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
            // Tangle.ExportStreams(@"C:\dm_streams\");
            Tangle.Dispose();
            base.TearDown();
        }

        [Test]
        public void OverwritingWithShorterStringWorks () {
            Scheduler.WaitFor(Tangle.Set("hello", "long string"));
            Assert.AreEqual("long string", Scheduler.WaitFor(Tangle.Get("hello")));
            Scheduler.WaitFor(Tangle.Set("hello", "world"));
            Assert.AreEqual("world", Scheduler.WaitFor(Tangle.Get("hello")));
        }

        [Test]
        public void OverwritingWithLongerStringWorks () {
            Scheduler.WaitFor(Tangle.Set("hello", "world"));
            Assert.AreEqual("world", Scheduler.WaitFor(Tangle.Get("hello")));
            Scheduler.WaitFor(Tangle.Set("hello", "long string"));
            Assert.AreEqual("long string", Scheduler.WaitFor(Tangle.Get("hello")));
        }

        [Test]
        public void UpdateGrowthWorks () {
            var s = "a";
            Scheduler.WaitFor(Tangle.Set("test", s));

            Tangle<string>.UpdateCallback callback = (str) => str + "a";

            for (int i = 0; i < 10; i++) {
                s = s + "a";

                Scheduler.WaitFor(Tangle.AddOrUpdate("test", null, callback));

                Assert.AreEqual(s, Scheduler.WaitFor(Tangle.Get("test")));
            }
        }

        [Test]
        public void UpdateShrinkageWorks () {
            var s = new String('a', 11);
            Scheduler.WaitFor(Tangle.Set("test", s));

            Tangle<string>.UpdateCallback callback = (str) => str.Substring(0, str.Length - 1);

            for (int i = 0; i < 10; i++) {
                s = s.Substring(0, s.Length - 1);

                Scheduler.WaitFor(Tangle.AddOrUpdate("test", null, callback));

                Assert.AreEqual(s, Scheduler.WaitFor(Tangle.Get("test")));
            }
        }
    }
}
