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
using System.Threading;
using NUnit.Framework;
using Squared.Data.Mangler.Internal;
using Squared.Data.Mangler.Serialization;
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
                serializer: BlittableSerializer<int>.Serialize,
                deserializer: BlittableSerializer<int>.Deserialize,
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
            Scheduler.WaitFor(Tangle.AddOrUpdate("a", 999, (ref int oldValue, ref int newValue) => false));

            Assert.AreEqual(1, Scheduler.WaitFor(Tangle.Get("a")));
        }

        [Test]
        public void AddOrUpdateCallbackCanMutateValue () {
            Scheduler.WaitFor(Tangle.Add("a", 1));
            Scheduler.WaitFor(Tangle.AddOrUpdate("a", 999, (ref int oldValue, ref int newValue) => {
                newValue = oldValue + 1;
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
                new TangleKey[] { "aa", "ea", "qa", "za" }, 
                Scheduler.WaitFor(Tangle.GetAllKeys())
            );
        }

        [Test]
        public void InsertInReverseOrder () {
            Scheduler.WaitFor(Tangle.Set("za", 4));
            Scheduler.WaitFor(Tangle.Set("qa", 3));
            Scheduler.WaitFor(Tangle.Set("ea", 2));
            Scheduler.WaitFor(Tangle.Set("aa", 1));

            Assert.AreEqual(
                new TangleKey[] { "aa", "ea", "qa", "za" },
                Scheduler.WaitFor(Tangle.GetAllKeys())
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
            Batch<int> batch = null;

            if (direction > 0)
                for (int i = 0; i < numIterations; i++) {
                    if (batch == null)
                        batch = Tangle.CreateBatch(batchSize);

                    batch.Add(i, i);

                    if (batch.Count == batchSize) {
                        yield return batch.Execute();
                        batch = null;
                    }
                }
            else
                for (int i = numIterations - 1; i >= 0; i--) {
                    if (batch == null)
                        batch = Tangle.CreateBatch(batchSize);

                    batch.Add(i, i);

                    if (batch.Count == batchSize) {
                        yield return batch.Execute();
                        batch = null;
                    }
                }

            if (batch != null)
                yield return batch.Execute();
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

        [Test]
        public void TestCount () {
            Assert.AreEqual(0, Tangle.Count);
            Scheduler.WaitFor(Tangle.Add(1, 1));
            Assert.AreEqual(1, Tangle.Count);
            Scheduler.WaitFor(Tangle.Add(2, 2));
            Assert.AreEqual(2, Tangle.Count);
            Scheduler.WaitFor(Tangle.Add(2, 2));
            Assert.AreEqual(2, Tangle.Count);
            Scheduler.WaitFor(Tangle.Set(1, 3));
            Assert.AreEqual(2, Tangle.Count);
        }

        [Test]
        public void TestMultiGet () {
            const int numValues = 500000;

            Scheduler.WaitFor(WriteLotsOfValuesInBatch(Tangle, numValues, -1));

            var keys = new List<int>();
            for (int i = 0; i < numValues; i += 2)
                keys.Add(i);

            long startTime = Time.Ticks;
            var fMultiGet = Tangle.Select(keys);
            var results = Scheduler.WaitFor(fMultiGet);
            decimal elapsedSeconds = (decimal)(Time.Ticks - startTime) / Time.SecondInTicks;
            Console.WriteLine(
                "Fetched {0} values in ~{1:00.000} second(s) at ~{2:00000.00} values/sec.",
                keys.Count, elapsedSeconds, keys.Count / elapsedSeconds
            );

            Assert.AreEqual(keys.Count, results.Count());

            Assert.AreEqual(
                keys.OrderBy((k) => k).ToArray(), 
                results.ToArray()
            );
        }

        [Test]
        public void TestMultiGetMissingValuesStillProduceAKeyValuePair () {
            var fMultiGet = Tangle.Select(new[] { 1, 2 });
            var results = Scheduler.WaitFor(fMultiGet);

            Assert.AreEqual(default(int), results[0]);
            Assert.AreEqual(default(int), results[1]);
        }

        [Test]
        public void TestGetAllValues () {
            const int numValues = 100000;

            Scheduler.WaitFor(WriteLotsOfValuesInBatch(Tangle, numValues, -1));

            long startTime = Time.Ticks;
            var fValues = Tangle.GetAllValues();
            var values = Scheduler.WaitFor(fValues);
            decimal elapsedSeconds = (decimal)(Time.Ticks - startTime) / Time.SecondInTicks;
            Console.WriteLine(
                "Fetched {0} values in ~{1:00.000} second(s) at ~{2:00000.00} values/sec.",
                numValues, elapsedSeconds, numValues / elapsedSeconds
            );

            Assert.AreEqual(numValues, values.Length);
            Assert.AreEqual(
                Enumerable.Range(0, numValues).ToArray(),
                values.OrderBy((v) => v).ToArray()
            );
        }

        [Test]
        public void TestJoin () {
            for (int i = 0; i < 8; i++)
                Scheduler.WaitFor(Tangle.Set(i, i * 2));

            using (var otherTangle = new Tangle<int>(Scheduler, new SubStreamSource(Storage, "2_", false))) {
                var keys = new List<string>();
                for (int i = 0; i < 8; i++) {
                    var key = new String((char)('a' + i), 1);
                    keys.Add(key);
                    Scheduler.WaitFor(otherTangle.Set(key, i));
                }

                var joinResult = Scheduler.WaitFor(
                    otherTangle.Join(
                        Tangle, keys,
                        (string leftKey, ref int leftValue) => 
                            leftValue,
                        (string leftKey, ref int leftValue, int rightKey, ref int rightValue) =>
                            new { leftKey, leftValue, rightKey, rightValue }
                    )
                );

                for (int i = 0; i < 8; i++) {
                    var item = joinResult[i];
                    Assert.AreEqual(keys[i], item.leftKey);
                    Assert.AreEqual(i, item.leftValue);
                    Assert.AreEqual(i, item.rightKey);
                    Assert.AreEqual(i * 2, item.rightValue);
                }
            }
        }

        [Test]
        public void TestSimpleJoin () {
            for (int i = 0; i < 8; i++)
                Scheduler.WaitFor(Tangle.Set(i, i * 2));

            using (var otherTangle = new Tangle<int>(Scheduler, new SubStreamSource(Storage, "2_", false))) {
                var keys = new List<string>();
                for (int i = 0; i < 8; i++) {
                    var key = new String((char)('a' + i), 1);
                    keys.Add(key);
                    Scheduler.WaitFor(otherTangle.Set(key, i));
                }

                var joinResult = Scheduler.WaitFor(
                    otherTangle.Join(
                        Tangle, keys,
                        (leftValue) => leftValue
                    )
                );

                for (int i = 0; i < 8; i++) {
                    var item = joinResult[i];
                    Assert.AreEqual(i, item.Key);
                    Assert.AreEqual(i * 2, item.Value);
                }
            }
        }

        [Test]
        public void TestCascadingSelect () {
            using (var otherTangle1 = new Tangle<int>(Scheduler, new SubStreamSource(Storage, "2_", false)))
            using (var otherTangle2 = new Tangle<int>(Scheduler, new SubStreamSource(Storage, "3_", false))) {
                Scheduler.WaitFor(Tangle.Set(1, 1));
                Scheduler.WaitFor(otherTangle1.Set(2, 3));
                Scheduler.WaitFor(otherTangle2.Set(2, 5));
                Scheduler.WaitFor(otherTangle2.Set(3, 4));

                var result = Scheduler.WaitFor(Tangle.CascadingSelect(
                    new [] { otherTangle1, otherTangle2 },
                    new [] { 1, 2, 3, 4 }
                ));

                Assert.AreEqual(new [] { 1, 3, 4, default(int) }, result);
            }
        }

        [Test]
        public void TestSelfJoin () {
            Scheduler.WaitFor(Tangle.Set(1, 2));
            Scheduler.WaitFor(Tangle.Set(2, 8));
            Scheduler.WaitFor(Tangle.Set(3, 4));
            Scheduler.WaitFor(Tangle.Set(4, 16));

            var joinResult = Scheduler.WaitFor(Tangle.Join(Tangle, new[] { 1, 3 }, (left) => left));
            
            Assert.AreEqual(2, joinResult[0].Key);
            Assert.AreEqual(8, joinResult[0].Value);
            Assert.AreEqual(4, joinResult[1].Key);
            Assert.AreEqual(16, joinResult[1].Value);
        }

        [Test]
        public void TestClear () {
            for (int i = 0; i < 100; i++)
                Scheduler.WaitFor(Tangle.Set(i, i));

            Scheduler.WaitFor(Tangle.Clear());

            for (int i = 0; i < 100; i++) {
                try {
                    Scheduler.WaitFor(Tangle.Find(i));
                    Assert.Fail("Expected to throw");
                } catch (FutureException fe) {
                    Assert.IsInstanceOf<KeyNotFoundException>(fe.InnerException);
                }

                try {
                    Scheduler.WaitFor(Tangle.Get(i));
                    Assert.Fail("Expected to throw");
                } catch (FutureException fe) {
                    Assert.IsInstanceOf<KeyNotFoundException>(fe.InnerException);
                }
            }

            Scheduler.WaitFor(Tangle.Set(2, 4));
            Assert.AreEqual(
                4, Scheduler.WaitFor(Tangle.Get(2))
            );
        }
    }

    [TestFixture]
    public class SynchronizationTests : BasicTestFixture {
        public Tangle<int> Tangle;

        [SetUp]
        public override void SetUp () {
            base.SetUp();

            Tangle = new Tangle<int>(
                Scheduler, Storage,
                serializer: BlittableSerializer<int>.Serialize,
                deserializer: BlittableSerializer<int>.Deserialize,
                ownsStorage: true
            );
        }

        [TearDown]
        public override void TearDown () {
            Tangle.Dispose();
            base.TearDown();
        }

        [Test]
        public void BarrierPreventsOperationsLaterInTheQueueFromCompleting () {
            var barrier = Tangle.CreateBarrier();
            var fOperation = Tangle.Add(1, 1);
            Scheduler.WaitFor(barrier);
            Assert.AreEqual(0, Tangle.Count);
            barrier.Open();
            Scheduler.WaitFor(fOperation);
            Assert.AreEqual(1, Tangle.Count);
        }

        [Test]
        public void DisposingOperationFutureCancelsTheOperation () {
            var barrier1 = Tangle.CreateBarrier(false);
            var fOperation = Tangle.Add(1, 1);
            var barrier2 = Tangle.CreateBarrier(true);
            fOperation.Dispose();
            barrier1.Open();
            Scheduler.WaitFor(barrier2);
            Assert.AreEqual(0, Tangle.Count);
        }

        [Test]
        public void DisposingTangleFailsPendingOperations () {
            var barrier = Tangle.CreateBarrier(false);
            var fOperation = Tangle.Add(1, 1);
            Scheduler.WaitFor(barrier);
            Tangle.Dispose();

            Assert.Throws<FutureDisposedException>(
                () => Scheduler.WaitFor(fOperation)
            );
        }

        [Test]
        public void BarrierCollectionOpensAllContainedBarriersWhenOpened () {
            var bc = new BarrierCollection(false, Tangle, Tangle);
            var fOperation = Tangle.Add(1, 1);

            Scheduler.WaitFor(bc);
            Assert.IsFalse(fOperation.Completed);
            bc.Open();
            Scheduler.WaitFor(fOperation);
            Assert.AreEqual(1, Tangle.Count);
        }
    }

    [TestFixture]
    public class StringTests : BasicTestFixture {
        public Tangle<string> Tangle;

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

            UpdateCallback<string> callback = (str) => str + "a";

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

            UpdateCallback<string> callback = (str) => str.Substring(0, str.Length - 1);

            for (int i = 0; i < 10; i++) {
                s = s.Substring(0, s.Length - 1);

                Scheduler.WaitFor(Tangle.AddOrUpdate("test", null, callback));

                Assert.AreEqual(s, Scheduler.WaitFor(Tangle.Get("test")));
            }
        }

        [Test]
        public void TestWastedDataBytes () {
            Assert.AreEqual(0, Tangle.WastedDataBytes);
            Scheduler.WaitFor(Tangle.Set(1, "abcd"));
            Assert.AreEqual(0, Tangle.WastedDataBytes);
            Scheduler.WaitFor(Tangle.Set(1, "abcdefgh"));
            Assert.AreEqual(4, Tangle.WastedDataBytes);
            Scheduler.WaitFor(Tangle.Set(1, "abc"));
            Assert.AreEqual(4, Tangle.WastedDataBytes);
            Scheduler.WaitFor(Tangle.Set(1, "abcdefgh"));
            Assert.AreEqual(4, Tangle.WastedDataBytes);
        }

        [Test]
        public void TestStoringHugeValue () {
            var hugeString = new String('a', 1024 * 1024 * 32);
            Scheduler.WaitFor(Tangle.Set(1, hugeString));
            Assert.AreEqual(hugeString, Scheduler.WaitFor(Tangle.Get(1)));
        }

        [Test]
        public void TestZeroByteValue () {
            var emptyString = "";
            Scheduler.WaitFor(Tangle.Set(1, emptyString));
            Assert.AreEqual(emptyString, Scheduler.WaitFor(Tangle.Get(1)));
        }

        [Test]
        public void TestGrowFromZeroBytes () {
            string emptyString = "", largerString = "abcdefgh";
            Scheduler.WaitFor(Tangle.Set(1, emptyString));
            Assert.AreEqual(emptyString, Scheduler.WaitFor(Tangle.Get(1)));
            Scheduler.WaitFor(Tangle.Set(1, largerString));
            Assert.AreEqual(largerString, Scheduler.WaitFor(Tangle.Get(1)));
            Assert.AreEqual(0, Tangle.WastedDataBytes);
        }

        [Test]
        public void TestRepeatedGrowAndShrink () {
            var wasted1 = Tangle.WastedDataBytes;

            for (int i = 1; i < 50; i += 10) {
                for (int j = 0; j < 20; j++) {
                    var key = new TangleKey(String.Format("test{0}", j));
                    var text = new String((char)(j + 63), i);

                    Scheduler.WaitFor(Tangle.Set(key, text));

                    Assert.AreEqual(
                        text, Scheduler.WaitFor(Tangle.Get(key))
                    );
                }
            }

            var wasted2 = Tangle.WastedDataBytes;
            Assert.Greater(wasted2, wasted1);

            for (int j = 0; j < 20; j++) {
                var key = new TangleKey(String.Format("test{0}", j));
                var text = new String((char)(j + 63), 5);

                Scheduler.WaitFor(Tangle.Set(key, text));

                Assert.AreEqual(
                    text, Scheduler.WaitFor(Tangle.Get(key))
                );
            }

            var wasted3 = Tangle.WastedDataBytes;
            Assert.AreEqual(wasted3, wasted2);

            for (int j = 20; j < 40; j++) {
                var key = new TangleKey(String.Format("test{0}", j));
                var text = new String((char)(j + 63), 10);

                Scheduler.WaitFor(Tangle.Set(key, text));

                Assert.AreEqual(
                    text, Scheduler.WaitFor(Tangle.Get(key))
                );
            }

            // This will fail if the freelist is not being used
            Assert.Less(Tangle.WastedDataBytes, wasted3);
        }

        [Test]
        public unsafe void TestLockExistingData () {
            var key = new TangleKey("test");
            var value = "abcdefgh";
            Scheduler.WaitFor(Tangle.Set(key, value));

            var findResult = Scheduler.WaitFor(Tangle.Find(key));
            var expectedBytes = Encoding.UTF8.GetBytes(value);

            fixed (byte * pExpected = expectedBytes)
            using (var data = Scheduler.WaitFor(findResult.LockData())) {
                Assert.AreEqual(expectedBytes.Length, data.Size);
                Assert.AreEqual(0, Native.memcmp(
                    pExpected, data.Pointer, 
                    new UIntPtr((uint)Math.Min(data.Size, expectedBytes.Length))
                ));
            }
        }

        [Test]
        public unsafe void TestGrowAndAlterDataByLockingIt () {
            var key = new TangleKey("test");
            var value = "abcdefgh";
            var newValue = "acdefghijkl";
            Scheduler.WaitFor(Tangle.Set(key, value));

            var findResult = Scheduler.WaitFor(Tangle.Find(key));
            var newBytes = Encoding.UTF8.GetBytes(newValue);

            fixed (byte* pNewBytes = newBytes)
            using (var data = Scheduler.WaitFor(findResult.LockData(newBytes.Length))) {
                Assert.GreaterOrEqual(data.Size, newBytes.Length);
                Native.memmove(data.Pointer, pNewBytes, new UIntPtr((uint)newBytes.Length));
            }

            Assert.AreEqual(newValue, Scheduler.WaitFor(Tangle.Get(key)));
        }
    }

    [TestFixture]
    public class KeyTests {
        [Test]
        public void TestKeyEquals () {
            var keyA = new TangleKey("abcd");
            var keyB = new TangleKey("abcd");
            var keyC = new TangleKey(2);
            var keyD = new TangleKey(2);

            Assert.IsTrue(keyA.Equals(keyA));
            Assert.IsTrue(keyA.Equals(keyB));
            Assert.IsTrue(keyC.Equals(keyC));
            Assert.IsTrue(keyC.Equals(keyD));
            Assert.IsFalse(keyA.Equals(keyC));
        }
    }
}
