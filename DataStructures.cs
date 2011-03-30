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

using System.Runtime.InteropServices;
using System.IO;

namespace Squared.Data.Mangler.Internal {
    [StructLayout(LayoutKind.Sequential, Pack = 1, Size = 128)]
    internal unsafe struct BTreeHeader {
        public static readonly uint Size;

        public long RootIndex;
        public long WastedDataBytes;
        public long ItemCount;
        public long MutationSentinel;

        static BTreeHeader () {
            Size = (uint)Marshal.SizeOf(typeof(BTreeHeader));
        }
    }

    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    internal unsafe struct BTreeNode {
        public static readonly uint Size;
        public static readonly uint TotalSize;
        public static readonly uint OffsetOfValues;
        public static readonly uint OffsetOfLeaves;

        public const int T = 32;
        public const int MaxValues = (2 * T) - 1;
        public const int MaxLeaves = MaxValues + 1;

        static BTreeNode () {
            if (T % 2 != 0)
                throw new InvalidDataException();

            Size = (uint)Marshal.SizeOf(typeof(BTreeNode));
            TotalSize = Size + (MaxValues * BTreeValue.Size) + (MaxLeaves * BTreeLeaf.Size);
            OffsetOfValues = Size;
            OffsetOfLeaves = OffsetOfValues + (MaxValues * BTreeValue.Size);
        }

        public byte IsValid;
        public byte HasLeaves;
        public ushort NumValues;
    }

    [StructLayout(LayoutKind.Sequential, Pack = 1, Size = 4)]
    internal unsafe struct BTreeLeaf {
        public static readonly uint Size;

        static BTreeLeaf () {
            Size = (uint)Marshal.SizeOf(typeof(BTreeLeaf));
        }

        public uint NodeIndex;

        public BTreeLeaf (long nodeIndex) {
            NodeIndex = (uint)nodeIndex;
        }
    }

    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    internal unsafe struct BTreeValue {
        public const int KeyPrefixSize = 4;
        public static readonly uint Size;

        static BTreeValue () {
            Size = (uint)Marshal.SizeOf(typeof(BTreeValue));
        }

        public uint DataOffset;
        public uint DataLength;
        public uint ExtraDataBytes;
        public uint KeyOffset;
        public ushort KeyLength;
        public ushort KeyType;
        public fixed byte KeyPrefix[KeyPrefixSize];
    }
}