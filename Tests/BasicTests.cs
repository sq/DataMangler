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
        public void BasicTest () {
            Scheduler.WaitFor(Tangle.Set("hello", 1));
            Assert.AreEqual(1, Scheduler.WaitFor(Tangle.Get("hello")));
        }
    }
}
