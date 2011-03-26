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
using System.IO;
using System.Linq;
using System.Text;

namespace Squared.Data.Mangler {
    public abstract class StreamSource : IDisposable {
        abstract internal Internal.StreamRef Open (string streamName);

        abstract public void Dispose ();
    }

    public abstract class CachingStreamSourceBase : StreamSource {
        protected readonly Dictionary<string, FileStream> Streams = new Dictionary<string, FileStream>();

        override internal Internal.StreamRef Open (string streamName) {
            FileStream result;

            if (!Streams.TryGetValue(streamName, out result)) {
                result = OpenStream(streamName);
                Streams[streamName] = result;
            }

            return new Internal.StreamRef(result, false);
        }

        protected abstract FileStream OpenStream (string streamName);

        override public void Dispose () {
            foreach (var stream in Streams.Values)
                stream.Dispose();
            Streams.Clear();
        }
    }

    public class SubStreamSource : StreamSource {
        private readonly StreamSource Inner;
        public readonly string Prefix;

        public SubStreamSource (StreamSource inner, string prefix) {
            Inner = inner;
            Prefix = prefix;
        }

        internal override Internal.StreamRef Open (string streamName) {
            return Inner.Open(Prefix + streamName);
        }

        public override void Dispose () {
            Inner.Dispose();
        }
    }

    public class AlternateStreamSource : CachingStreamSourceBase {
        public readonly string Filename;

        public AlternateStreamSource (string filename) {
            Filename = filename;
        }

        protected override FileStream OpenStream (string streamName) {
            return Internal.Native.OpenAlternateStream(Filename, streamName);
        }
    }

    public class FolderStreamSource : CachingStreamSourceBase {
        private string _Folder;

        public FolderStreamSource (string folder) {
            _Folder = folder;
        }

        protected static string GetPath (string folder, string streamName) {
            return Path.Combine(folder, streamName);
        }

        protected override FileStream OpenStream (string streamName) {
            var path = GetPath(_Folder, streamName);
            return File.Open(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Delete | FileShare.ReadWrite);
        }

        public string Folder {
            get {
                return _Folder;
            }
            set {
                if (value == _Folder)
                    return;

                var oldFolder = _Folder;

                Dispose();

                Directory.CreateDirectory(value);

                foreach (var filename in Directory.GetFiles(oldFolder)) {
                    var newFilename = Path.Combine(value, Path.GetFileName(filename));
                    if (File.Exists(newFilename))
                        File.Delete(newFilename);

                    File.Move(filename, newFilename);
                }

                _Folder = value;

                Directory.Delete(oldFolder, true);
            }
        }
    }
}
