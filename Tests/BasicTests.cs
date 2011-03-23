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
    }
}
