using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RaptorDB.Common
{
    static class BitHelper
    {
        public static void FillArray<T>(T[] arr, T value)
        {
            for (int i = 0; i < arr.Length; i++)
            {
                arr[i] = value;
            }
        }
        public static T[] Repeat<T>(T value, int len)
        {
            var a = new T[len];
            FillArray(a, value);
            return a;
        }
        public static int BitCount(uint n)
        { // 32-bit recursive reduction using SWAR
            n -= ((n >> 1) & 0x55555555);
            n = (((n >> 2) & 0x33333333) + (n & 0x33333333));
            n = (((n >> 4) + n) & 0x0f0f0f0f);
            return (int)((n * 0x01010101) >> 24);
        }
        public static void BitmapSet(uint[] bitmap, int index, bool val)
        {
            int pointer = index >> 5;
            uint mask = (uint)1 << (31 - (index % 32));

            if (val)
                bitmap[pointer] |= mask;
            else
                bitmap[pointer] &= ~mask;
        }
        public static unsafe void BitmapSet(void* bitmap, int index, bool val)
        {
            int pointer = index / 8;
            byte mask = (byte)(1 << (7 - (index % 8)));

            if (val)
                *((byte*)bitmap + pointer) |= mask;
            else
                *((byte*)bitmap + pointer) &= (byte)~mask;
        }

        public static bool BitmapGet(uint[] bitmap, int index)
        {
            int pointer = index >> 5;
            uint mask = (uint)1 << (31 - (index % 32));

            if (pointer < bitmap.Length)
                return (bitmap[pointer] & mask) != 0;
            else
                return false;
        }

        public static void AndArray(uint[] a, uint[] b)
        {
            var len = Math.Min(a.Length, b.Length);
            for (int i = 0; i < len; i++)
            {
                a[i] &= b[i];
            }
        }

        public static void AndNotArray(uint[] a, uint[] b)
        {
            var len = Math.Min(a.Length, b.Length);
            for (int i = 0; i < len; i++)
            {
                a[i] &= ~b[i];
            }
        }

        public static void OrArray(uint[] a, uint[] b)
        {
            var len = Math.Min(a.Length, b.Length);
            for (int i = 0; i < len; i++)
            {
                a[i] |= b[i];
            }
        }

        public static void XorArray(uint[] a, uint[] b)
        {
            var len = Math.Min(a.Length, b.Length);
            for (int i = 0; i < len; i++)
            {
                a[i] ^= b[i];
            }
        }

        public static IEnumerable<int> GetBitIndexes(uint[] bitmap)
        {
            for (int i = 0; i < bitmap.Length; i++)
            {
                var w = bitmap[i];
                if (w > 0)
                {
                    for (int j = 0; j < 32; j++)
                    {
                        if ((w & 1) > 0)
                            yield return (i << 5) + j;
                        w >>= 1;
                    }
                }
            }
        }
    }
}
