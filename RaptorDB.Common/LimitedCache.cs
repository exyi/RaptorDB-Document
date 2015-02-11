using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace RaptorDB.Common
{
    public class LimitedCache<T>
    {
        public static int MaxInitCount = 128;
        private long _maxSum;
        private long _sum;
        private int _maxCount;
        Func<T, long> _getNum;
        T[] _arr = new T[1];
        int index = 0;
        int removeIndex = 0;

        public LimitedCache(long maxSum, int maxCount, Func<T, long> getNum)
        {
            this._getNum = getNum;
            _maxSum = maxSum;
            _maxCount = maxCount;
            if (maxCount > MaxInitCount)
                _arr = new T[MaxInitCount];
            else
                _arr = new T[maxCount];
        }

        public void Add(T item)
        {
            var n = _getNum(item);
            index++;
            if (index >= _arr.Length)
            {
                if (_arr.Length < _maxCount)
                {
                    var nsize = _arr.Length * 2;
                    if (nsize > _maxCount) nsize = _maxCount;
                    ChangeArrSize(nsize);
                }
                else index = 0;
            }
            _arr[index] = item;
            _sum += n;
            while (_sum > _maxSum)
            {
                removeIndex = removeIndex + 1 % _arr.Length;
                var i = _arr[removeIndex];
                if (i != null) _sum -= _getNum(i);
            }
        }

        protected void ChangeArrSize(int size)
        {
            var a = new T[size];
            _arr.CopyTo(a, 0);
            _arr = a;
        }
    }
}
