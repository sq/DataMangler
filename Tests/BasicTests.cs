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
    [TestFixture]
    public class BasicTests {
        public const string TestFile = @"C:\test.dmdb";

        public TaskScheduler Scheduler;
        public Tangle<int> Tangle;

        [SetUp]
        public void SetUp () {
            Scheduler = new TaskScheduler();
            if (File.Exists(TestFile))
                File.Delete(TestFile);
            Tangle = new Tangle<int>(Scheduler, TestFile);
        }

        [TearDown]
        public void TearDown () {
            Tangle.ExportStreams(@"C:\test.dmdb_streams\");
            Scheduler.WaitFor(Tangle.Dispose());
            Scheduler.Dispose();
        }

        [Test]
        public void CanGetValueByNameAfterSettingIt () {
            Scheduler.WaitFor(Tangle.Set("hello", 1));
            Assert.AreEqual(1, Scheduler.WaitFor(Tangle.Get("hello")));
        }

        [Test]
        public void NonExistentKeysThrow () {
            try {
                Scheduler.WaitFor(Tangle.Get("missing"));
                Assert.Fail("Should have thrown");
            } catch (FutureException fe) {
                Assert.IsInstanceOf<KeyNotFoundException>(fe.InnerException);
            } catch {
                throw;
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

        protected IEnumerator<object> WriteLotsOfValues (Tangle<int> tangle, int numIterations, int batchSize) {
            var futures = new List<IFuture>();

            for (int i = 0; i < numIterations; i++) {
                var key = new TangleKey(i);
                var f = tangle.Set(key, i);
                futures.Add(f);

                if (futures.Count >= batchSize) {
                    yield return Future.WaitForAll(futures);
                    futures.Clear();
                }
            }
        }

        [Test]
        public void CanWriteLotsOfValuesSequentially () {
            const int numValues = 10000;

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

        protected IEnumerator<object> CheckLotsOfValues (Tangle<int> tangle, int numIterations) {
            for (int i = 0; i < numIterations; i++) {
                var key = new TangleKey(i);
                var f = tangle.Get(key);
                yield return f;
                Assert.AreEqual(i, f.Result);
            }
        }
    }
}
