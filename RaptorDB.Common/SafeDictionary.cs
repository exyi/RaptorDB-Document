using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Runtime.InteropServices;
using System.Text;
using System.Linq;

namespace RaptorDB.Common
{
    public class SafeDictionary<TKey, TValue>
    {
        private readonly object _Padlock = new object();
        private readonly Dictionary<TKey, TValue> _Dictionary;

        public SafeDictionary(int capacity)
        {
            _Dictionary = new Dictionary<TKey, TValue>(capacity);
        }

        public SafeDictionary()
        {
            _Dictionary = new Dictionary<TKey, TValue>();
        }

        public bool TryGetValue(TKey key, out TValue value)
        {
            lock (_Padlock)
                return _Dictionary.TryGetValue(key, out value);
        }

        public TValue this[TKey key]
        {
            get
            {
                lock (_Padlock)
                    return _Dictionary[key];
            }
            set
            {
                lock (_Padlock)
                    _Dictionary[key] = value;
            }
        }

        public int Count
        {
            get { lock (_Padlock) return _Dictionary.Count; }
        }

        public ICollection<KeyValuePair<TKey, TValue>> GetList()
        {
            return (ICollection<KeyValuePair<TKey, TValue>>)_Dictionary;
        }

        public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
        {
            return ((ICollection<KeyValuePair<TKey, TValue>>)_Dictionary).GetEnumerator();
        }

        public void Add(TKey key, TValue value)
        {
            lock (_Padlock)
            {
                if (_Dictionary.ContainsKey(key) == false)
                    _Dictionary.Add(key, value);
                else
                    _Dictionary[key] = value;
            }
        }

        public TKey[] Keys()
        {
            lock (_Padlock)
            {
                TKey[] keys = new TKey[_Dictionary.Keys.Count];
                _Dictionary.Keys.CopyTo(keys, 0);
                return keys;
            }
        }

        public bool Remove(TKey key)
        {
            lock (_Padlock)
            {
                return _Dictionary.Remove(key);
            }
        }
    }

    public class SafeSortedList<T, V>
    {
        private object _padlock = new object();
        SortedList<T, V> _list = new SortedList<T, V>();

        public int Count
        {
            get { lock (_padlock) return _list.Count; }
        }

        public void Add(T key, V val)
        {
            lock (_padlock)
                _list.Add(key, val);
        }

        public void Remove(T key)
        {
            if (key == null)
                return;
            lock (_padlock)
                _list.Remove(key);
        }

        public T GetKey(int index)
        {
            lock (_padlock) return _list.Keys[index];
        }

        public V GetValue(int index)
        {
            lock (_padlock) return _list.Values[index];
        }
    }

    //------------------------------------------------------------------------------------------------------------------

    public static class FastDateTime
    {
        public static TimeSpan LocalUtcOffset;

        public static DateTime Now
        {
            get { return DateTime.UtcNow + LocalUtcOffset; }
        }

        static FastDateTime()
        {
            LocalUtcOffset = TimeZone.CurrentTimeZone.GetUtcOffset(DateTime.Now);
        }
    }


    //------------------------------------------------------------------------------------------------------------------

    public static class Helper
    {
        public static MurmurHash2Unsafe MurMur = new MurmurHash2Unsafe();

        public static unsafe bool Cmp(byte[] a, byte[] b)
        {
            if (a.Length != b.Length)
                return false;
            fixed (byte* aptr = a)
            {
                fixed (byte* bptr = b)
                {
                    return Cmp(aptr, bptr, a.Length);
                }
            }
        }

        public static unsafe bool Cmp(byte* ptra, byte* ptrb, int len)
        {
            while (len >= 8)
            {
                if (*(long*)ptra != *(long*)ptrb)
                    return false;
                ptra += 8;
                ptrb += 8;
                len -= 8;
            }
            while (len >= 4)
            {
                if (*(int*)ptra != *(int*)ptrb)
                    return false;
                ptra += 4;
                ptrb += 4;
                len -= 4;
            }
            while (len > 0)
            {
                if (*ptra != *ptrb)
                    return false;
                ptrb++;
                ptra++;
                len--;
            }
            return true;
        }
        

        public static int Log2(int n)
        {
            int bits = 0;
            if (n > 32767)
            {
                n >>= 16;
                bits += 16;
            }
            if (n > 127)
            {
                n >>= 8;
                bits += 8;
            }
            if (n > 7)
            {
                n >>= 4;
                bits += 4;
            }
            if (n > 1)
            {
                n >>= 2;
                bits += 2;
            }
            if (n > 0)
            {
                bits++;
            }
            return bits;
        }

        public static int ToInt32(byte[] value, int startIndex, bool reverse)
        {
            if (reverse)
            {
                byte[] b = new byte[4];
                Buffer.BlockCopy(value, startIndex, b, 0, 4);
                Array.Reverse(b);
                return ToInt32(b, 0);
            }

            return ToInt32(value, startIndex);
        }

        public static unsafe int ToInt32(byte[] value, int startIndex)
        {
            fixed (byte* numRef = &(value[startIndex]))
            {
                return *((int*)numRef);
            }
        }

        public static long ToInt64(byte[] value, int startIndex, bool reverse)
        {
            if (reverse)
            {
                byte[] b = new byte[8];
                Buffer.BlockCopy(value, startIndex, b, 0, 8);
                Array.Reverse(b);
                return ToInt64(b, 0);
            }
            return ToInt64(value, startIndex);
        }

        public static unsafe long ToInt64(byte[] value, int startIndex)
        {
            fixed (byte* numRef = &(value[startIndex]))
            {
                return *(((long*)numRef));
            }
        }

        public static short ToInt16(byte[] value, int startIndex, bool reverse)
        {
            if (reverse)
            {
                byte[] b = new byte[2];
                Buffer.BlockCopy(value, startIndex, b, 0, 2);
                Array.Reverse(b);
                return ToInt16(b, 0);
            }
            return ToInt16(value, startIndex);
        }

        public static unsafe short ToInt16(byte[] value, int startIndex)
        {
            fixed (byte* numRef = &(value[startIndex]))
            {
                return *(((short*)numRef));
            }
        }

        public static unsafe byte[] GetBytes(long num, bool reverse)
        {
            byte[] buffer = new byte[8];
            fixed (byte* numRef = buffer)
            {
                *((long*)numRef) = num;
            }
            if (reverse)
                Array.Reverse(buffer);
            return buffer;
        }

        public static unsafe byte[] GetBytes(int num, bool reverse)
        {
            byte[] buffer = new byte[4];
            fixed (byte* numRef = buffer)
            {
                *((int*)numRef) = num;
            }
            if (reverse)
                Array.Reverse(buffer);
            return buffer;
        }

        public static unsafe byte[] GetBytes(short num, bool reverse)
        {
            byte[] buffer = new byte[2];
            fixed (byte* numRef = buffer)
            {
                *((short*)numRef) = num;
            }
            if (reverse)
                Array.Reverse(buffer);
            return buffer;
        }

        public static byte[] GetBytes(string s)
        {
            return Encoding.UTF8.GetBytes(s);
        }

        public static string GetString(byte[] buffer, int index, int length)
        {
            return Encoding.UTF8.GetString(buffer, index, length);
        }
    }
}
