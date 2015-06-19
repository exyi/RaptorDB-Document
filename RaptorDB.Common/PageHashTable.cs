using GenericPointerHelpers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection.Emit;
using System.Runtime.InteropServices;
using System.Text;
using static RaptorDB.Common.PageHashTableHelper;
using System.Collections;

namespace RaptorDB.Common
{
    public unsafe class PageHashTable<TKey, TValue> : IDisposable, IDictionary<TKey, TValue>
    {

        public readonly int Capacity;
        public readonly int KeySize;
        public readonly IPageSerializer<TKey> KeySerializer;
        public readonly int ValueSize;
        public readonly IPageSerializer<TValue> ValueSerializer;
        public readonly int EntrySize;
        public readonly int ClusterSize;
        public readonly uint Seed = 0xc58f1a7b;
        public readonly byte* StartPointer;
        private readonly bool dealloc;
        private int count = 0;

        public PageHashTable(int count, IPageSerializer<TKey> keySerializer, IPageSerializer<TValue> valueSerializer,
            byte* startPointer = null,
            int clusterSize = 16)
        {
            this.Capacity = count;
            this.KeySerializer = keySerializer;
            this.ValueSerializer = valueSerializer;

            KeySize = keySerializer == null ? GenericPointerHelper.SizeOf<TKey>() : keySerializer.Size;
            ValueSize = valueSerializer == null ? GenericPointerHelper.SizeOf<TValue>() : valueSerializer.Size;
            EntrySize = KeySize + ValueSize + 1;
            ClusterSize = clusterSize;

            if (startPointer == null)
            {
                StartPointer = (byte*)Marshal.AllocHGlobal(count * EntrySize).ToPointer();
                dealloc = true;
            }
            else
            {
                StartPointer = startPointer;
                GC.SuppressFinalize(this);
            }
        }

        public int FindEntry(byte[] key, bool stopOnDeleted)
        {
            if (key.Length != KeySize) throw new ArgumentException("wrong key length");
            fixed (byte* keyPtr = key)
            {
                var hash = Helper.MurMur.Hash(keyPtr, KeySize, Seed);
                byte* resultPtr;
                return FindEntry(keyPtr, hash, stopOnDeleted, out resultPtr);
            }
        }

        protected int FindEntry(byte* key, uint hash, bool stopOnDeleted, out byte* pointer)
        {
            byte stopMap = stopOnDeleted ? (byte)3 : (byte)1;
            byte hashMap = (byte)(hash | 3);
            int hashIndex = ((int)hash & 0x7fffffff) % Capacity;
            var clusterOffset = hashIndex % ClusterSize;
            var clusterIndex = hashIndex / ClusterSize;
            int diffPlus = -1;
            do
            {
                var ptr = pointer = StartPointer + (EntrySize * hashIndex);
                for (int i = 0; i < ClusterSize; i++)
                {
                    byte flags = *ptr;
                    // flags: 
                    //     [0] (1b): value
                    //     [1] (1b): deleted => empty = 00, value = 01, deleted = 10
                    //     [2] (6b): first 6 hash bits
                    if ((flags | 2) == hashMap && Helper.Cmp(ptr + 1, key, KeySize))
                    {
                        return hashIndex + i % Capacity;
                    }
                    if ((flags & stopMap) == 0)
                    {
                        return -(hashIndex + i + 1) % Capacity;
                    }
                    if (hashIndex + i == Capacity) ptr = StartPointer;
                    else ptr += EntrySize;
                }
                if (diffPlus == -1)
                    diffPlus = (int)((hash * 41) % Capacity) | 1;
                clusterIndex = (clusterIndex + diffPlus) % (Capacity / ClusterSize);
                hashIndex = clusterIndex + clusterOffset;
            }
            while (true);
        }

        private void SetEntry(byte* ptr, uint keyHash, byte* key, TValue value)
        {
            *ptr = (byte)((keyHash & ~3) | 1);
            ptr++;
            GenericPointerHelper.CopyBytes(key, ptr, (uint)KeySize);
            if (ValueSerializer != null) ValueSerializer.Save(ptr + KeySize, value);
            else GenericPointerHelper.Write(ptr, value);
        }

        public TValue Get(TKey key)
        {
            TValue value;
            TryGetValue(key, out value);
            return value;
        }

        public bool TryGetValue(TKey key, out TValue value)
        {
            fixed (byte* kptr = new byte[KeySize])
            {
                if (KeySerializer != null)
                    KeySerializer.Save(kptr, key);
                else GenericPointerHelper.Write(kptr, key);
                var hash = Helper.MurMur.Hash(kptr, KeySize, Seed);
                byte* pointer;
                if (FindEntry(kptr, hash, false, out pointer) >= 0)
                {
                    if (ValueSerializer != null)
                        value = ValueSerializer.Read(pointer + 1 + KeySize);
                    else value = GenericPointerHelper.Read<TValue>(pointer + 1 + KeySize);
                    return true;
                }
                else
                {
                    value = default(TValue);
                    return false;
                }
            }
        }

        /// <summary>
        /// Sets the value
        /// </summary>
        /// <returns>if something was replaced</returns>
        public bool Set(TKey key, TValue value, bool allowOverwrite = true)
        {
            fixed (byte* kptr = new byte[KeySize])
            {
                if (KeySerializer != null)
                    KeySerializer.Save(kptr, key);
                else GenericPointerHelper.Write(kptr, key);
                var hash = Helper.MurMur.Hash(kptr, KeySize, Seed);
                byte* pointer;
                var index = FindEntry(kptr, hash, true, out pointer);
                if (index < 0)
                {
                    if (!allowOverwrite) throw new ArgumentException("An item with the same key already exists");
                    count++;
                }

                SetEntry(pointer, hash, kptr, value);

                return index < 0;
            }
        }

        private void Remove(byte* ptr)
        {
            *ptr = 2;
            GenericPointerHelper.InitMemory(ptr + 1, (uint)(KeySize + ValueSize), 0);
        }

        void ReadEntry(byte* ptr, out TKey key, out TValue value)
        {
            if (KeySerializer != null)
                key = KeySerializer.Read(ptr + 1);
            else key = GenericPointerHelper.Read<TKey>(ptr + 1);

            if (ValueSerializer != null)
                value = ValueSerializer.Read(ptr + 1 + KeySize);
            else value = GenericPointerHelper.Read<TValue>(ptr + 1 + KeySize);
        }


        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (dealloc)
                Marshal.FreeHGlobal(new IntPtr(StartPointer));
        }

        public void CopyTo(TKey[] keys, TValue[] values)
        {
            var ptr = StartPointer;
            for (int i = 0, hIndex = 0; hIndex < Capacity; hIndex++)
            {
                if ((*ptr & 1) == 1)
                {
                    TKey key;
                    TValue value;
                    ReadEntry(ptr, out key, out value);
                    if (values != null && values.Length > i) values[i] = value;
                    if (keys != null && keys.Length > i) keys[i] = key;
                    i++;
                }
                ptr++;
            }
        }

        #region IDictionary

        public ICollection<TKey> Keys
        {
            get
            {
                var keys = new TKey[count];
                CopyTo(keys, null);
                return keys;
            }
        }

        public ICollection<TValue> Values
        {
            get
            {
                var values = new TValue[count];
                CopyTo(null, values);
                return values;
            }
        }

        public int Count
        {
            get
            {
                return count;
            }
        }

        public bool IsReadOnly
        {
            get
            {
                throw new NotImplementedException();
            }
        }

        public TValue this[TKey key]
        {
            get
            {
                return Get(key);
            }

            set
            {
                Set(key, value);
            }
        }

        public bool ContainsKey(TKey key)
        {
            fixed (byte* kptr = new byte[KeySize])
            {
                if (KeySerializer != null)
                    KeySerializer.Save(kptr, key);
                else
                    GenericPointerHelper.Write(kptr, key);
                var hash = Helper.MurMur.Hash(kptr, KeySize, Seed);
                byte* pointer;
                return FindEntry(kptr, hash, false, out pointer) >= 0;
            }
        }

        public void Add(TKey key, TValue value)
        {
            Set(key, value, false);
        }

        public bool Remove(TKey key)
        {
            fixed (byte* kptr = new byte[KeySize])
            {
                if (KeySerializer != null) KeySerializer.Save(kptr, key);
                else GenericPointerHelper.Write(kptr, key);
                var hash = Helper.MurMur.Hash(kptr, KeySize, Seed);
                byte* pointer;
                if (FindEntry(kptr, hash, true, out pointer) >= 0)
                {
                    Remove(pointer);
                    count--;
                    return true;
                }
            }
            return false;
        }

        public void Add(KeyValuePair<TKey, TValue> item)
        {
            Set(item.Key, item.Value, false);
        }

        public void Clear()
        {
            GenericPointerHelper.InitMemory(StartPointer, (uint)(Capacity * EntrySize), 0);
        }

        public bool Contains(KeyValuePair<TKey, TValue> item)
        {
            TValue value;
            if (TryGetValue(item.Key, out value))
            {
                return value.Equals(item.Value);
            }
            return false;
        }

        public void CopyTo(KeyValuePair<TKey, TValue>[] array, int arrayIndex)
        {
            var ptr = StartPointer;
            for (int i = arrayIndex, hIndex = 0; hIndex < Capacity && i < array.Length; hIndex++)
            {
                if ((*ptr & 1) == 1)
                {
                    TKey key;
                    TValue value;
                    ReadEntry(ptr, out key, out value);
                    array[i] = new KeyValuePair<TKey, TValue>(key, value);
                    i++;
                }
                ptr += EntrySize;
            }
        }

        public bool Remove(KeyValuePair<TKey, TValue> item)
        {
            if (Contains(item))
                return Remove(item.Key);
            return false;
        }

        public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
        {
            var array = new KeyValuePair<TKey, TValue>[count];
            CopyTo(array, 0);
            return ((IEnumerable<KeyValuePair<TKey, TValue>>)array).GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
        #endregion

        ~PageHashTable()
        {
            Dispose(false);
        }
    }
    public unsafe static class PageHashTableHelper
    {
        public unsafe interface IPageSerializer<T>
        {
            int Size { get; }
            T Read(byte* ptr);
            void Save(byte* ptr, T value);
        }
        public unsafe class StructPageSerializer<T> : IPageSerializer<T> where T : struct
        {
            private readonly int size = GenericPointerHelper.SizeOf<T>();
            public int Size { get { return size; } }

            public T Read(byte* ptr)
            {
                return GenericPointerHelper.Read<T>(ptr);
            }

            public void Save(byte* ptr, T value)
            {
                GenericPointerHelper.Write<T>(ptr, value);
                //Marshal.StructureToPtr(value, new IntPtr(ptr), false);
            }
        }

        public unsafe class StringPageSerializer : IPageSerializer<string>
        {
            public StringPageSerializer(int size, Encoding encoding = null)
            {
                this.size = size;
                this.StringEncoding = encoding ?? Encoding.UTF8;
            }
            private readonly int size;
            public int Size { get { return size; } }
            public Encoding StringEncoding;

            public string Read(byte* ptr)
            {
                return new string((sbyte*)ptr, 0, size, StringEncoding);
            }

            public void Save(byte* ptr, string value)
            {
                fixed (char* strptr = value)
                {
                    StringEncoding.GetBytes(strptr, value.Length, ptr, Size);
                }
            }
        }


        public unsafe class UnicodeStringPageSerializer : IPageSerializer<string>
        {
            public UnicodeStringPageSerializer(int size)
            {
                this.size = size;
            }
            private readonly int size;
            public int Size { get { return size * 2; } }
            public Encoding StringEncoding;

            public string Read(byte* ptr)
            {
                return new string((sbyte*)ptr, 0, size, StringEncoding);
            }

            public void Save(byte* ptr, string value)
            {
                char* ptrchar = (char*)ptr;
                fixed (char* strptrFix = value)
                {
                    char* strptr = strptrFix;
                    for (int i = 0; i < size; i++)
                    {
                        *ptrchar++ = *strptr++;
                    }
                }
            }
        }

        public static PageHashTable<string, TStructValue> CreateStringKey<TStructValue>(int count, int stringLength, byte* startPointer = null)
            where TStructValue : struct
        {
            return new PageHashTable<string, TStructValue>(count,
                new StringPageSerializer(stringLength),
                null,
                startPointer);
        }

        public static PageHashTable<TKey, TValue> CreateStructStruct<TKey, TValue>(int count, byte* startPointer = null)
            where TValue : struct
            where TKey : struct
        {
            return new PageHashTable<TKey, TValue>(count,
                //new StructPageSerializer<TKey>(), new StructPageSerializer<TValue>(), 
                null, null,
                startPointer);
        }

        public static PageHashTable<string, string> CreateStringString<TStructValue>(int count, int keyLength, int valueLength, byte* startPointer = null)
            where TStructValue : struct
        {
            return new PageHashTable<string, string>(count,
                new StringPageSerializer(keyLength),
                new StringPageSerializer(valueLength),
                startPointer);
        }

        //public static class TypeHelper<T>
        //{
        //    private static Func<int> CreateSizeOfFunction()
        //    {
        //        var method = new DynamicMethod("_", typeof(int), new Type[0]);
        //        var il = method.GetILGenerator();
        //        il.Emit(OpCodes.Sizeof, typeof(T));
        //        il.Emit(OpCodes.Ret);
        //        return (Func<int>)method.CreateDelegate(typeof(Func<int>));
        //    }

        //    private static BinaryWriter<T> CreateWriter()
        //    {
        //        var method = new DynamicMethod("_", typeof(void), new Type[] { typeof(byte*), typeof(T) });
        //        var il = method.GetILGenerator();
        //        il.Emit(OpCodes.Nop);
        //        il.Emit(OpCodes.Ldarg_0);
        //        il.Emit(OpCodes.Ldarg_1);
        //        if(typeof(T) == typeof(int))
        //            il.Emit(OpCodes.Stind_I4);
        //        else il.Emit(OpCodes.Stobj, typeof(T));
        //        il.Emit(OpCodes.Ret);
        //        return (BinaryWriter<T>)method.CreateDelegate(typeof(BinaryWriter<>).MakeGenericType(typeof(T)));
        //    }

        //    private static BinaryReader<T> CreateReader()
        //    {
        //        var method = new DynamicMethod("_", typeof(T), new Type[] { typeof(byte*)});
        //        var il = method.GetILGenerator();
        //        il.Emit(OpCodes.Ldarg_0);
        //        if (typeof(T) == typeof(int))
        //            il.Emit(OpCodes.Ldind_I4);
        //        else il.Emit(OpCodes.Ldobj, typeof(T));
        //        il.Emit(OpCodes.Ret);
        //        return (BinaryReader<T>)method.CreateDelegate(typeof(BinaryReader<T>));
        //    }

        //    public static readonly int SizeOf = CreateSizeOfFunction()();
        //    public static readonly int MarshalSizeOf = Marshal.SizeOf(typeof(T));
        //    public static readonly BinaryWriter<T> Writer = CreateWriter();
        //    public static readonly BinaryReader<T> Reader = CreateReader();
        //}
        //public delegate void BinaryWriter<Tb>(byte* ptr, Tb value);
        //public delegate Tb BinaryReader<Tb>(byte* ptr);
    }
}
