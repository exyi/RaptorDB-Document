using GenericPointerHelpers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection.Emit;
using System.Runtime.InteropServices;
using System.Text;
using static RaptorDB.Common.PageHashTableHelper;

namespace RaptorDB.Common
{
    public unsafe class PageHashTable<TKey, TValue> : IDisposable
    {

        public readonly int Count;
        public readonly int KeySize;
        public readonly IPageSerializer<TKey> KeySerializer;
        public readonly int ValueSize;
        public readonly IPageSerializer<TValue> ValueSerializer;
        public readonly int EntrySize;
        public readonly int ClusterSize;
        public readonly uint Seed = 0xc58f1a7b;
        public readonly byte* StartPointer;
        private readonly bool dealloc;

        public PageHashTable(int count, IPageSerializer<TKey> keySerializer, IPageSerializer<TValue> valueSerializer,
            byte* startPointer = null,
            int clusterSize = 16)
        {
            this.Count = count;
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
            int hashIndex = ((int)hash & 0x7fffffff) % Count;
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
                        return hashIndex + i % Count;
                    }
                    if ((flags & stopMap) == 0)
                    {
                        return -(hashIndex + i + 1) % Count;
                    }
                    if (hashIndex + i == Count) ptr = StartPointer;
                    else ptr += EntrySize;
                }
                if (diffPlus == -1)
                    diffPlus = (int)((hash * 41) % Count) | 1;
                clusterIndex = (clusterIndex + diffPlus) % (Count / ClusterSize);
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
            fixed (byte* kptr = new byte[KeySize])
            {
                if (KeySerializer != null)
                    KeySerializer.Save(kptr, key);
                else
                    GenericPointerHelper.Write(kptr, key);
                var hash = Helper.MurMur.Hash(kptr, KeySize, Seed);
                byte* pointer;
                if (FindEntry(kptr, hash, false, out pointer) >= 0)
                {
                    if (KeySerializer != null)
                        return ValueSerializer.Read(pointer + 1 + KeySize);
                    return GenericPointerHelper.Read<TValue>(pointer + 1 + KeySize);
                }
                else return default(TValue);
            }
        }
        /// <summary>
        /// Sets value
        /// </summary>
        /// <returns>if something was replaced</returns>
        public bool Set(TKey key, TValue value)
        {
            fixed (byte* kptr = new byte[KeySize])
            {
                if (KeySerializer != null)
                    KeySerializer.Save(kptr, key);
                else GenericPointerHelper.Write(kptr, key);
                var hash = Helper.MurMur.Hash(kptr, KeySize, Seed);
                byte* pointer;
                var index = FindEntry(kptr, hash, true, out pointer);

                SetEntry(pointer, hash, kptr, value);

                return index < 0;
            }
        }

        public TValue this[TKey key]
        {
            get { return Get(key); }
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
            where TValue: struct
            where TKey: struct
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
