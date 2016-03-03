using System;
using System.Collections.Generic;
using System.Text;
using System.Collections;
using System.Runtime.CompilerServices;
using System.Diagnostics;
using System.Linq;
using System.Diagnostics.Contracts;

namespace RaptorDB.Common
{
    public enum WahBitArrayState : byte
    {
        Bitarray = 0,
        Wah = 1,
        Indexes = 2,
        Index = 3,
    }
    public class WahBitArray
    {
        public const int WahPerformanceRatio = 5;
        public const int BitmapPerformanceRatio = 1;
        public WahBitArray(int index = -1)
        {
            state = WahBitArrayState.Index;
            singleIndex = index;
        }

        public WahBitArray(WahBitArrayState type, uint[] ints)
        {
            state = type & (WahBitArrayState)4;
            def = ((byte)type & 4) != 0;
            switch (state)
            {
                case WahBitArrayState.Wah:
                    wah = ints;
                    break;
                case WahBitArrayState.Bitarray:
                    bitmap = ints;
                    break;
                case WahBitArrayState.Indexes:
                    offsets = new HashSet<uint>(ints);
                    break;
                case WahBitArrayState.Index:
                    this.singleIndex = (int)ints[0];
                    break;
            }
        }

        /// <summary>
        /// Creates new instance of WahBitArray in 'Indexes' state with specified values
        /// </summary>
        public static WahBitArray FromIndexes(int[] ints, bool def = false)
        {
            var wah = new WahBitArray();
            wah.def = def;
            wah.offsets = new HashSet<uint>();
            foreach (var i in ints) wah.offsets.Add((uint)i);
            return wah;
        }

        private HashSet<uint> offsets;
        private uint[] wah;
        private uint[] bitmap;
        private uint currentMax = 0;
        private int singleIndex;
        private WahBitArrayState state;
        private bool def = false;
        public bool isDirty = false;

        /// <summary>
        /// Clones the bitarray
        /// </summary>
        public WahBitArray Copy()
        {
            if (state == WahBitArrayState.Wah)
            {
                var c = new uint[wah.Length];
                Array.Copy(wah, c, c.Length);
                return new WahBitArray(WahBitArrayState.Wah, c) { def = def };
            }
            else if (state == WahBitArrayState.Bitarray)
            {
                var c = new uint[bitmap.Length];
                Array.Copy(bitmap, c, c.Length);
                return new WahBitArray(WahBitArrayState.Bitarray, c) { def = def };
            }
            else if (state == WahBitArrayState.Indexes)
            {
                return new WahBitArray(WahBitArrayState.Indexes, new uint[0])
                {
                    offsets = new HashSet<uint>(offsets),
                    def = def
                };
            }
            else if (state == WahBitArrayState.Index)
            {
                return new WahBitArray(singleIndex) { def = def };
            }
            else throw new NotSupportedException("invalid bitarray state");
        }

        public int GetFirstIndex()
        {
            if (state == WahBitArrayState.Indexes)
            {
                return (int)offsets.Min();
            }
            return GetBitIndexes().First();
        }

        /// <summary>
        /// Gets bit value at specified index
        /// if in <see cref="WahBitArrayState.Wah"/> state the array is decompressed
        /// </summary>
        public bool Get(int index)
        {
            if (state == WahBitArrayState.Indexes)
                return offsets.Contains((uint)index) != def;
            else if (state == WahBitArrayState.Index) return (singleIndex == index) != def;

            DecompressWah();

            return BitHelper.BitmapGet(bitmap, index) != def;
        }

        /// <summary>
        /// Sets bit value at specified index
        /// if in <see cref="WahBitArrayState.Wah"/> state the array is decompressed and switched to bitmap state
        /// </summary>
        public void Set(int index, bool val)
        {
            if (state == WahBitArrayState.Indexes)
            {
                if (val != def)
                {
                    isDirty |= offsets.Add((uint)index);
                    // set max
                    if (index > currentMax)
                        currentMax = (uint)index;
                }
                else
                {
                    isDirty |= offsets.Remove((uint)index);
                }

                ChangeTypeIfNeeded();
                return;
            }
            if (state == WahBitArrayState.Index)
            {
                if (index == singleIndex && val == def)
                {
                    singleIndex = -1;
                    isDirty = true;
                }
                else if (index != singleIndex && val != def)
                {
                    isDirty = true;
                    state = WahBitArrayState.Indexes;
                    offsets = new HashSet<uint>() { (uint)index, (uint)singleIndex };
                }
            }
            if (state != WahBitArrayState.Bitarray)
                DecompressWah();
            isDirty = true;
            if (index > bitmap.Length * 32)
            {
                if (val == def) return;
                ResizeBitmap(index);
            }
            BitHelper.BitmapSet(bitmap, index, val);
        }

        [Obsolete]
        public int Length
        {
            get
            {
                if (state == WahBitArrayState.Index)
                {
                    return singleIndex + 1;
                }
                else if (state == WahBitArrayState.Indexes)
                {
                    return (int)currentMax;
                }
                DecompressWah();
                return bitmap.Length / 32;
            }
        }

        #region bit operations

        [Pure]
        public static uint[] CloneArray(uint[] arr)
        {
            var na = new uint[arr.Length];
            Buffer.BlockCopy(arr, 0, na, 0, arr.Length * 4);
            return na;
        }

        public static WahBitArray GenericBitOp(WahBitArray a, WahBitArray b,
            Func<bool, bool, bool> op,
            Action<uint[], uint[]> bitBitOp,
            Action<uint[], uint[]> bitWahOp = null,
            Func<uint[], uint[], uint[]> wahWahOp = null,
            Action<uint[], HashSet<uint>> bitIndexOp = null,
            Action<HashSet<uint>, HashSet<uint>> indexIndexOp = null,
            Func<HashSet<uint>, HashSet<uint>, HashSet<uint>> indexIndexImmutableOp = null,
            bool inPlace = false)
        {
            var sta = a.state;
            var stb = b.state;
            uint[] result = null;
            WahBitArrayState resultSt = WahBitArrayState.Bitarray;

            if (sta == WahBitArrayState.Wah && stb == WahBitArrayState.Wah && wahWahOp != null)
            {
                // Wah + Wah
                result = wahWahOp(a.wah, b.wah);
                resultSt = WahBitArrayState.Wah;
                goto Finalize;
            }
            if (sta == WahBitArrayState.Bitarray && stb == WahBitArrayState.Wah && bitWahOp != null)
            {
                // Bitamp + Wah
                result = inPlace ? a.bitmap : CloneArray(a.bitmap);
                bitWahOp(a.bitmap, b.wah);
                goto Finalize;
            }
            if (stb == WahBitArrayState.Bitarray && sta == WahBitArrayState.Wah && bitWahOp != null)
            {
                // Wah + Bitmap
                result = inPlace ? b.bitmap : CloneArray(b.bitmap);
                bitWahOp(result, a.wah);
                goto Finalize;
            }
            if (sta == WahBitArrayState.Bitarray && stb == WahBitArrayState.Indexes && bitIndexOp != null)
            {
                result = inPlace ? a.bitmap : CloneArray(a.bitmap);
                bitIndexOp(result, b.offsets);
                goto Finalize;
            }
            if (stb == WahBitArrayState.Bitarray && sta == WahBitArrayState.Indexes && bitIndexOp != null)
            {
                result = inPlace ? b.bitmap : CloneArray(b.bitmap);
                bitIndexOp(result, a.offsets);
                goto Finalize;
            }
            if(sta == WahBitArrayState.Indexes && sta == WahBitArrayState.Indexes)
            {
                if(indexIndexOp != null)
                {
                    resultSt = WahBitArrayState.Indexes;

                }
            }

            Finalize:
            // T+
            // TODO: implement
            throw new NotImplementedException();
        }

        public WahBitArray And(WahBitArray op, bool inPlace = false)
        {
            return GenericBitOp(this, op,
                (a, b) => a & b,
                BitHelper.AndArray,
                wahWahOp: WahHelper.WahAnd);
            // TODO: WAH
        }

        public WahBitArray AndNot(WahBitArray op, bool inPlace = false)
        {
            return GenericBitOp(this, op,
                (a, b) => a & !b,
                BitHelper.AndNotArray);
        }

        public WahBitArray Or(WahBitArray op, bool inPlace = false)
        {
            return GenericBitOp(this, op,
                (a, b) => a | b,
                BitHelper.OrArray);
        }

        public WahBitArray Xor(WahBitArray op, bool inPlace = false)
        {
            return GenericBitOp(this, op,
                (a, b) => a ^ b,
                BitHelper.XorArray);
        }

        public WahBitArray Not(bool cloneBitmap = true)
        {
            if (cloneBitmap)
            {
                var c = Copy();
                c.def = !def;
                return c;
            }
            else
            {
                switch (state)
                {
                    case WahBitArrayState.Bitarray:
                        return new WahBitArray(state, bitmap) { def = !def };
                    case WahBitArrayState.Wah:
                        return new WahBitArray(state, wah) { def = !def };
                    case WahBitArrayState.Indexes:
                        return new WahBitArray() { state = WahBitArrayState.Indexes, offsets = offsets, def = !def };
                    case WahBitArrayState.Index:
                        return new WahBitArray(singleIndex) { def = !def };
                    default:
                        throw new NotSupportedException();
                }
            }
        }

        #endregion

        /// <summary>
        /// Counts all ones (!= def) in the bitmap
        /// </summary>
        public long CountOnes()
        {
            if (state == WahBitArrayState.Index)
            {
                return singleIndex < 0 ? 0 : 1;
            }
            if (state == WahBitArrayState.Indexes)
            {
                return offsets.Count;
            }
            if (state == WahBitArrayState.Wah)
            {
                int c = 0;
                foreach (var i in wah)
                    c += WahHelper.BitCount(i);
                return c;
            }
            if (state == WahBitArrayState.Bitarray)
            {
                long c = 0;
                foreach (uint i in bitmap)
                    c += BitHelper.BitCount(i);
                return c;
            }
            throw new NotSupportedException();
        }

        /// <summary>
        /// If the state is <see cref="WahBitArrayState.Bitarray"/> the bitmap is compressed
        /// </summary>
        public void CompressBitmap()
        {
            if (state == WahBitArrayState.Bitarray)
            {
                if (bitmap != null)
                {
                    wah = Compress(bitmap);
                    bitmap = null;
                    state = WahBitArrayState.Wah;
                }
            }
        }

        /// <summary>
        /// Decompress wah bitmap and assigns it to 'bitmap'
        /// State is switched to bitmap only if 'bitmap.Length * bitmapRatio &lt; wah.Length * wahRatio'
        /// </summary>
        /// <returns>if the state was switched</returns>
        public bool CompressAndSwitchBetterState(int wahRatio = WahPerformanceRatio, int bitmapRatio = BitmapPerformanceRatio)
        {
            if (state != WahBitArrayState.Bitarray) throw new InvalidOperationException("operation valid only in Bitmap state");

            bitmap = Uncompress(wah);
            if (bitmap.Length * bitmapRatio < wah.Length * wahRatio)
            {
                state = WahBitArrayState.Bitarray;
                return true;
            }
            return false;
        }

        /// <summary>
        /// Gets compressed array using the best method
        /// </summary>
        public uint[] GetCompressed(out WahBitArrayState type)
        {
            type = WahBitArrayState.Wah;

            if (state == WahBitArrayState.Indexes)
            {
                if (offsets.Count * 32 > currentMax)
                {
                    type = WahBitArrayState.Wah;
                    return WahHelper.FromIndexes(GetOffsets());
                }
                else
                {
                    type = WahBitArrayState.Indexes;
                    return GetOffsets();
                }
            }
            else if (state == WahBitArrayState.Wah)
            {
                return wah;
            }
            // TODO: cache the wah bitmap
            return Compress(bitmap);
        }

        /// <summary>
        /// Gets indices of bits not equal to def
        /// </summary>
        public IEnumerable<int> GetBitIndexes()
        {
            if (state == WahBitArrayState.Index && singleIndex >= 0)
            {
                return new[] { singleIndex };
            }
            else if (state == WahBitArrayState.Indexes)
            {
                return GetOffsets().Cast<int>();
            }
            else if (state == WahBitArrayState.Wah)
            {
                return WahHelper.BitIndexes(wah);
            }
            else if (state == WahBitArrayState.Bitarray)
            {
                return BitHelper.GetBitIndexes(bitmap);
            }
            throw new NotSupportedException();
        }

        /// <summary>
        /// Gets ordered offsets from 'offsets'
        /// </summary>
        public uint[] GetOffsets(bool sorted = true)
        {
            Debug.Assert(state == WahBitArrayState.Indexes);
            var k = new uint[offsets.Count];
            offsets.CopyTo(k, 0);
            if (sorted) Array.Sort(k);
            return k;
        }

        #region [  P R I V A T E  ]

        [Obsolete]
        private void prelogic(WahBitArray op, out uint[] left, out uint[] right)
        {
            this.DecompressWah();

            left = this.GetBitArray();
            right = op.GetBitArray();
            int ic = left.Length;
            int uc = right.Length;
            if (ic > uc)
            {
                uint[] ar = new uint[ic];
                right.CopyTo(ar, 0);
                right = ar;
            }
            else if (ic < uc)
            {
                uint[] ar = new uint[uc];
                left.CopyTo(ar, 0);
                left = ar;
            }
        }

        /// <summary>
        /// Gets pure bitarray
        /// </summary>
        public uint[] GetBitArray()
        {
            if (state == WahBitArrayState.Indexes)
                return UnpackOffsets(offsets, (int)currentMax);

            DecompressWah(switchState: false);
            uint[] ui = new uint[bitmap.Length];
            bitmap.CopyTo(ui, 0);

            return ui;
        }

        /// <summary>
        /// returns offsets unpacked as bitarray
        /// </summary>
        public static uint[] UnpackOffsets(IEnumerable<uint> offsets, int len)
        {

            uint[] bitmap = new uint[(len + 31) / 32];
            foreach (int index in offsets)
            {
                if (index < len)
                {
                    BitHelper.BitmapSet(bitmap, index, true);
                }
            }

            return bitmap;
        }

        public const int BitmapOffsetSwitchOverCount = 10;
        /// <summary>
        /// Changes type to bitarray from offsets if it is good idea
        /// </summary>
        private void ChangeTypeIfNeeded()
        {
            if (state != WahBitArrayState.Indexes)
                return;

            var bitmapLength = (int)(currentMax / 32) + 1;
            int c = offsets.Count;
            if (c > bitmapLength && c > BitmapOffsetSwitchOverCount)
            {
                state = WahBitArrayState.Bitarray;
                if (bitmap == null || bitmap.Length < bitmapLength) bitmap = new uint[bitmapLength];
                else Array.Clear(bitmap, 0, bitmap.Length);
                // populate bitmap
                foreach (var i in offsets)
                    BitHelper.BitmapSet(bitmap, (int)i, true);
                // clear list
                offsets = null;
            }
        }

        /// <summary>
        /// Resizes a bitmap array to size 'length * (2 ^ k)' >= required to store 'index' (for lowest integer 'k') 
        /// </summary>
        private void ResizeBitmap(int index)
        {
            Debug.Assert(state == WahBitArrayState.Bitarray);
            if (bitmap == null)
            {
                bitmap = new uint[index >> 5];
            }
            else
            {
                var len = bitmap.Length;
                while (len * 32 < index) len *= 2;
                if (len > bitmap.Length)
                {
                    uint[] ar = new uint[index >> 5];
                    bitmap.CopyTo(ar, 0);
                    bitmap = ar;
                }
            }
        }

        /// <summary>
        /// decompresses to BitArray state from WAH
        /// </summary>
        private void DecompressWah(bool switchState = true)
        {
            if (state == WahBitArrayState.Bitarray)
                return;

            if (state == WahBitArrayState.Wah)
            {
                bitmap = Uncompress(wah);
                if (switchState)
                {
                    state = WahBitArrayState.Bitarray;
                    wah = null;
                }
            }
        }
        #endregion

        #region compress / uncompress
        /// <summary>
        /// Takes 31 bit block at bit index
        /// </summary>
        public static uint Take31Bits(uint[] data, int index)
        {
            ulong l1 = 0;
            ulong l2 = 0;
            ulong l = 0;
            ulong ret = 0;
            int off = (index % 32);
            int pointer = index >> 5;

            l1 = data[pointer];
            pointer++;
            if (pointer < data.Length)
                l2 = data[pointer];

            l = (l1 << 32) + l2;
            ret = (l >> (33 - off)) & 0x7fffffff;

            return (uint)ret;
        }

        public static uint[] Compress(uint[] data)
        {
            List<uint> compressed = new List<uint>();
            uint zeros = 0;
            uint ones = 0;
            int count = data.Length << 5;
            for (int i = 0; i < count;)
            {
                uint num = Take31Bits(data, i);
                i += 31;
                if (num == 0) // all zero
                {
                    // FIX: 31WAH
                    zeros += 1;
                    FlushOnes(compressed, ref ones);
                }
                else if (num == 0x7fffffff) // all ones
                {
                    // FIX: 31WAH
                    ones += 1;
                    FlushZeros(compressed, ref zeros);
                }
                else // literal
                {
                    FlushOnes(compressed, ref ones);
                    FlushZeros(compressed, ref zeros);
                    compressed.Add(num);
                }
            }
            FlushOnes(compressed, ref ones);
            FlushZeros(compressed, ref zeros);
            return compressed.ToArray();
        }

        public static uint[] Uncompress(uint[] data)
        {
            int index = 0;
            List<uint> list = new List<uint>();
            if (data == null)
                return null;

            foreach (uint ci in data)
            {
                if ((ci & 0x80000000) == 0) // literal
                {
                    Write31Bits(list, index, ci);
                    // FIX: 31WAH
                    index += 31;
                }
                else
                {
                    uint count = (ci & 0x3fffffff) * 31;
                    if ((ci & 0x40000000) > 0) // ones count
                        WriteOnes(list, index, count);

                    index += (int)count;
                }
            }
            if (list.Count * 32 < index) list.AddRange(new uint[index / 32 - list.Count]);
            return list.ToArray();
        }

        private static void FlushOnes(List<uint> compressed, ref uint ones)
        {
            if (ones > 0)
            {
                uint n = 0xc0000000 | ones;
                ones = 0;
                compressed.Add(n);
            }
        }

        private static void FlushZeros(List<uint> compressed, ref uint zeros)
        {
            if (zeros > 0)
            {
                uint n = 0x80000000 | zeros;
                zeros = 0;
                compressed.Add(n);
            }
        }

        private static void EnsureLength(List<uint> l, int index)
        {
            if (l.Count * 31 < index) l.AddRange(new uint[index / 31 - l.Count + 1]);
        }


        private static void Write31Bits(List<uint> list, int index, uint val)
        {
            EnsureLength(list, index + 31);

            int off = (index % 32);
            int pointer = index >> 5;

            if (pointer >= list.Count - 1)
                list.Add(0);

            ulong l = ((ulong)list[pointer] << 32) + list[pointer + 1];
            l |= (ulong)val << (33 - off);

            list[pointer] = (uint)(l >> 32);
            list[pointer + 1] = (uint)(l & 0xffffffff);
        }

        private static void WriteOnes(List<uint> list, int index, uint count)
        {
            if (list.Count * 32 < index) list.AddRange(new uint[list.Count - index / 32]);

            int off = index % 32;
            int pointer = index >> 5;
            int ccount = (int)count;
            int indx = index;
            int x = 32 - off;

            if (pointer >= list.Count)
                list.Add(0);

            if (ccount > x || x == 32) //current pointer
            {
                list[pointer] |= (uint)((0xffffffff >> off));
                ccount -= x;
                indx += x;
            }
            else
            {
                list[pointer] |= (uint)((0xffffffff << ccount) >> off);
                ccount = 0;
            }

            bool checklast = true;
            while (ccount >= 32)//full ints
            {
                if (checklast && list[list.Count - 1] == 0)
                {
                    list.RemoveAt(list.Count - 1);
                    checklast = false;
                }

                list.Add(0xffffffff);
                ccount -= 32;
                indx += 32;
            }
            int p = indx >> 5;
            off = indx % 32;
            if (ccount > 0)
            {
                uint i = 0xffffffff << (32 - ccount);
                if (p > (list.Count - 1)) //remaining
                    list.Add(i);
                else
                    list[p] |= (uint)(i >> off);
            }
        }
        #endregion
    }
}
