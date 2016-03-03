using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace RaptorDB.Common
{
    struct WahWriter
    {
        List<uint[]> bits;
        int index;
        uint[] arr;
        readonly int len;
        int lastSumVal;
        public WahWriter(int capacity)
        {
            lastSumVal = -1;
            len = capacity;
            arr = new uint[len];
            index = -1;
            bits = null;
        }
        void AddChunk()
        {
            index = 0;
            if (bits == null) bits = new List<uint[]>();
            bits.Add(arr);
            arr = new uint[len];
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void Add(uint val)
        {
            index++;
            if (len == index) AddChunk();
            arr[index] = val;
        }
        public void WriteLit(uint val)
        {
            if (val == 0)
            {
                if (lastSumVal == 0) arr[index]++;
                else
                {
                    Add((1u << 31) | (0u << 30) | 1u);
                    lastSumVal = 0;
                }
            }
            else if (val == 0x7fffffff)
            {
                if (lastSumVal == 1) arr[index]++;
                else
                {
                    Add((1u << 31) | (1u << 30) | 1u);
                    lastSumVal = 1;
                }
            }
            else
            {
                lastSumVal = -1;
                Add(val);
            }
        }
        /// <param name="value">0/1</param>
        public void WriteSum(uint len, uint value)
        {
            if (lastSumVal != value)
            {
                Add((1u << 31) | (value << 30) | len);
                lastSumVal = (int)value;
            }
            else
            {
                arr[index] += len;
            }
        }
        public uint[] ToArray()
        {
            int bc = 0;
            uint[] wharr = null;
            if (bits != null)
            {
                bc = bits.Count * len;
                wharr = new uint[bc + index + 1];
                for (int i = 0; i < bits.Count; i++)
                {
                    Buffer.BlockCopy(bits[i], 0, wharr, i * len * 4, len * 4);
                }
            }
            wharr = wharr ?? new uint[index + 1];
            Array.Copy(arr, 0, wharr, bc, index + 1);
            return wharr;
        }
    }
}
