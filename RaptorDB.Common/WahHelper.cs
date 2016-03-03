using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace RaptorDB.Common
{

    static class WahHelper
    {
        public const uint SecondBitMask = 1u << 30;
        public const uint FirstBitMask = 1u << 31;
        public const uint CountBitMask = ~(3 << 30);

        struct IterState
        {
            public uint[] Arr;
            public int Index;
            public int Count;
            public uint Value;
            public uint TakeWord()
            {
                if (Count == 0)
                {
                    uint ai = Arr[Index++];
                    if ((ai >> 31) == 1)
                    {
                        Count = (int)((ai & 0x3fffffff) - 1);
                        Value = (ai & SecondBitMask >> 30) * 0x7fffffff;
                    }
                    else
                    {
                        Value = ai;
                    }
                }
                else Count--;
                return Value;
            }
            public void Skip(int len)
            {
                Count -= len;
                while (Count < 0)
                {
                    var ai = Arr[Index++];
                    if ((ai & FirstBitMask) != 0)
                    {
                        Count += (int)(ai & 0x3fffffff - 1);
                        if (Count >= 0) Value = (ai & SecondBitMask >> 30) * 0x7fffffff;
                    }
                    else
                    {
                        Count++;
                        Value = ai;
                    }
                }
            }
            public void CopyTo(WahWriter w, int len)
            {
                while (len > 0)
                {
                    var ai = Arr[Index++];
                    if ((ai >> 31) == 1)
                    {
                        Count = (int)(ai & 0x3fffffff);
                        len -= Count;
                        Value = (ai & SecondBitMask >> 30);
                        w.WriteSum((uint)Count, Value);
                        Value *= 0x7fffffff;
                    }
                    else
                    {
                        Count = 0;
                        len--;
                        w.WriteLit(Value = ai);
                    }
                }
                Count += len;
            }
            public IterState(uint[] arr)
            {
                this.Arr = arr;
                Index = 0;
                Count = 0;
                Value = 0;
            }
        }

        public static uint[] WahNot(uint[] bitmap)
        {
            var result = new uint[bitmap.Length];
            for (int i = 0; i < bitmap.Length; i++)
            {
                if ((bitmap[i] & FirstBitMask) == 0)
                    result[i] = bitmap[i] ^ 0x7fffffffu;
                else result[i] = bitmap[i] ^ SecondBitMask;
            }
            return result;
        }

        public static void InPlaceNot(uint[] wah)
        {
            for (int i = 0; i < wah.Length; i++)
            {
                if ((wah[i] & FirstBitMask) == 0)
                    wah[i] ^= 0x7fffffffu;
                else wah[i] ^= SecondBitMask;
            }
        }

        public static uint[] WahAnd(uint[] ap, uint[] bp)
        {
            var a = new IterState(ap);
            var b = new IterState(bp);
            var w = new WahWriter(ap.Length);
            while (a.Index < ap.Length && b.Index < bp.Length)
            {
                w.WriteLit(a.TakeWord() & b.TakeWord());
                if (a.Count > 0)
                {
                    if (a.Value == 0) b.Skip(a.Count);
                    else b.CopyTo(w, a.Count);
                    a.Count = 0;
                }
                if (b.Count > 0)
                {
                    if (b.Value == 0) a.Skip(b.Count);
                    else a.CopyTo(w, b.Count);
                    b.Count = 0;
                }
            }
            return w.ToArray();
        }

        public static uint[] Compress(uint[] arr)
        {
            var w = new WahWriter(4096);
            var len = arr.Length;
            var i = 0;
            bool last;
            do
            {
                w.WriteLit(Take31Bits(arr, len, i, out last));
                i++;
            } while (!last);
            return w.ToArray();
        }

        public static IEnumerable<int> BitIndexes(uint[] wah)
        {
            var index = 0;
            for (int i = 0; i < wah.Length; i++)
            {
                var w = wah[i];
                if ((w & FirstBitMask) > 0) // wah word with ones
                {
                    var count = (int)(w & CountBitMask) * 31;
                    index += count;
                    if ((w & SecondBitMask) > 0)
                    {
                        for (var j = 0; j < count; j++)
                        {
                            yield return j + index;
                        }
                    }
                }
                else
                {
                    for (int j = 0; j < 31; j++)
                    {
                        if ((w & 1) > 0)
                            yield return index + j;
                        w >>= 1;
                    }
                    index += 31;
                }
            }
        }

        /// <summary>
        /// Takes 31 bit block at block index
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static uint Take31Bits(uint[] arr, int len, int index, out bool last)
        {
            // index from the end of word
            // if (!last & arr[wordIndex] == 0 && arr[wordIndex + 1] == 0) return 0;
            var wordIndex = index - ((index + 31) >> 5);
            var bitIndex = 33 - ((32 - index) & 31);
            last = wordIndex + 1 == len;
            if (!last)
            {
                long words = ((long)arr[wordIndex] << 32) | arr[wordIndex + 1];
                return (uint)((words >> bitIndex) & 0x7fffffffu);
            }
            else
            {
                if (bitIndex == 33) last = false;
                return (arr[wordIndex] << 32 - bitIndex) & 0x7fffffffu;
            }
        }

        public static uint[] FromIndexes(uint[] offsets)
        {
            var w = new WahWriter(4096);
            uint index = 0;
            uint map = 0;
            for (int i = 0; i < offsets.Length; i++)
            {
                var o = offsets[i];
                if (o / 32 != index)
                {
                    if (map != 0) { w.WriteLit(map); index++; }
                    if (index < o)
                    {
                        w.WriteSum(o - index, 0);
                        index = o;
                    }
                    map = 0;
                }
                map |= (uint)1 << (int)(31 - (index % 32));
            }
            return w.ToArray();
        }

        public static int BitCount(uint element)
        {
            if ((element & FirstBitMask) == 0) return BitHelper.BitCount(element);
            if ((element & SecondBitMask) == 0) return 0;
            else return (int)(element & CountBitMask) * 32;
        }
    }
}
