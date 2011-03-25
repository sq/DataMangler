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
