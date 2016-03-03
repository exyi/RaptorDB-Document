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
using System.Threading;
using System.Runtime.CompilerServices;
using System.Diagnostics;

namespace RaptorDB.Common
{
    public unsafe class PageHashTableBase<TKey, TValue> : IDisposable, IEnumerable<KeyValuePair<TKey, TValue>>
    {

        public readonly long Capacity;
        public readonly int KeySize;
        public readonly IPageSerializer<TKey> KeySerializer;
        public readonly int ValueSize;
        public readonly IPageSerializer<TValue> ValueSerializer;
        public readonly int EntrySize;
        public readonly int ClusterSize;
        public readonly uint Seed = 0xc58f1a7b;
        public readonly byte* StartPointer;
        public readonly bool AllowDuplicates;
        public readonly ReaderWriterLockSlim rwlock = new ReaderWriterLockSlim();
        protected readonly bool dealloc;
        protected int count = 0;
        // protected int uniqCount = 0;

        // sets are not thread safe, so one buffer is enough
        protected byte[] setKeyBuffer;

        protected SimpleBufferManager readKeyBufferManager;


        public PageHashTableBase(long capacity,
            IPageSerializer<TKey> keySerializer,
            IPageSerializer<TValue> valueSerializer,
            byte* startPointer,
            int clusterSize,
            bool allowDups,
            int count = 0)
        {
            // TODO: smarter capacity to be sure that nothing wrong will happen

            if (capacity % clusterSize != 0) throw new ArgumentException(string.Format("capacity ({0}) must be divisible by cluster size ({1})", capacity, clusterSize));

            this.Capacity = capacity;
            this.KeySerializer = keySerializer;
            this.ValueSerializer = valueSerializer;

            KeySize = keySerializer == null ? GenericPointerHelper.SizeOf<TKey>() : keySerializer.Size;
            ValueSize = valueSerializer == null ? GenericPointerHelper.SizeOf<TValue>() : valueSerializer.Size;
            EntrySize = KeySize + ValueSize + 1;
            ClusterSize = clusterSize;
            AllowDuplicates = allowDups;

            if (startPointer == null)
            {
                StartPointer = (byte*)Marshal.AllocHGlobal(new IntPtr(capacity * EntrySize)).ToPointer();
                dealloc = true;
            }
            else
            {
                StartPointer = startPointer;
                GC.SuppressFinalize(this);
            }

            this.count = count;

            setKeyBuffer = new byte[KeySize];
            readKeyBufferManager = new SimpleBufferManager(KeySize);
        }

        public long FindEntry(byte[] key, bool stopOnDeleted)
        {
            if (key.Length != KeySize) throw new ArgumentException("wrong key length");
            fixed (byte* keyPtr = key)
            {
                var hash = Helper.MurMur.Hash(keyPtr, KeySize, Seed);
                byte* resultPtr;
                return FindEntry(keyPtr, hash, stopOnDeleted, out resultPtr);
            }
        }

        protected void WriteKey(byte* pointer, TKey value)
        {
            if (KeySerializer != null)
                KeySerializer.Save(pointer, value);
            else GenericPointerHelper.Write(pointer, value);
        }

        protected TValue ReadValue(byte* pointer)
        {
            if (ValueSerializer != null)
                return ValueSerializer.Read(pointer);
            else return GenericPointerHelper.Read<TValue>(pointer);
        }

        protected TKey ReadKey(byte* pointer)
        {
            if (KeySerializer != null)
                return KeySerializer.Read(pointer);
            else return GenericPointerHelper.Read<TKey>(pointer);
        }

        protected long FindEntry(byte* key, uint hash, bool insert, out byte* pointer)
        {
            // on insert: stop on first entry without value
            // on lookup: stop on first entry without continuation
            var stopMap = insert ? 1u : 3u;
            // on insert & AllowDuplicates: return only on the last value
            var returnMap = (insert && AllowDuplicates) ? 0u : 2u;
            var hashMap = ((hash & 0xfc) | 1 | returnMap);
            long hashIndex = ((int)hash & 0x7fffffff) % Capacity;
            var clusterOffset = hashIndex % ClusterSize;
            var clusterIndex = hashIndex / ClusterSize;
            int diffPlus = -1;
            do
            {
                pointer = StartPointer + (EntrySize * hashIndex);
                for (int i = 0; i < ClusterSize; i++)
                {
                    if (hashIndex + i == Capacity) pointer = StartPointer;
                    uint flags = *pointer;
                    // flags: 
                    //     [0] (1b): value
                    //     [1] (1b): continue search => empty = 00, (last) value = 01, deleted = 10, value (and not last) = 11
                    //     [2] (6b): first 6 hash bits
                    if ((flags & stopMap) == 0)
                    {
                        return -(((hashIndex + i) % Capacity) + 1);
                    }
                    if ((flags | returnMap) == hashMap && Helper.Cmp(pointer + 1, key, KeySize))
                    {
                        return hashIndex + i % Capacity;
                    }
                    pointer += EntrySize;
                }
                if (diffPlus == -1)
                    diffPlus = (int)((hash * 41) % Capacity) | 1;
                clusterIndex = (clusterIndex + diffPlus) % (Capacity / ClusterSize);
                hashIndex = clusterIndex * ClusterSize + clusterOffset;
            }
            while (true);
        }

        protected long FindNextEntry(byte* key, uint hash, ref byte* pointer)
        {
            byte hashMap = (byte)(hash | 3);
            var clusterOffset = ((int)hash & 0x7fffffff) % ClusterSize;
            var startingAt = (int)(((pointer - StartPointer) / EntrySize - clusterOffset) % ClusterSize);
            var hashIndex = (pointer - StartPointer) / EntrySize - startingAt;
            var clusterIndex = hashIndex / ClusterSize;
            int diffPlus = -1;


            startingAt++;
            pointer += EntrySize;
            do
            {
                for (var i = startingAt; i < ClusterSize; i++)
                {
                    if (hashIndex + i == Capacity)
                        pointer = StartPointer;
                    byte flags = *pointer;
                    // flags: 
                    //     [0] (1b): value
                    //     [1] (1b): continue search => empty = 00, (last) value = 01, deleted = 10, value (and not last) = 11
                    //     [2] (6b): first 6 hash bits
                    if (flags == 0)
                    {
                        return -(((hashIndex + i) % Capacity) + 1);
                    }
                    else if ((flags | 2) == hashMap && Helper.Cmp(pointer + 1, key, KeySize))
                    {
                        return hashIndex + i % Capacity;
                    }

                    pointer += EntrySize;
                }
                if (diffPlus == -1)
                    diffPlus = (int)((hash * 41) % Capacity) | 1;
                clusterIndex = (clusterIndex + diffPlus) % (Capacity / ClusterSize);
                hashIndex = clusterIndex * ClusterSize + clusterOffset;
                pointer = StartPointer + (EntrySize * hashIndex);
                startingAt = 0;
            }
            while (true);
        }

        protected long FindEntry(byte* key, uint hash, TValue value, out byte* pointer)
        {
            // on insert & AllowDuplicates: return only on the last value
            var hashMap = ((hash & 0xfc) | 3);
            long hashIndex = ((int)hash & 0x7fffffff) % Capacity;
            var clusterOffset = hashIndex % ClusterSize;
            var clusterIndex = hashIndex / ClusterSize;
            int diffPlus = -1;
            do
            {
                pointer = StartPointer + (EntrySize * hashIndex);
                for (int i = 0; i < ClusterSize; i++)
                {
                    if (hashIndex + i == Capacity) pointer = StartPointer;
                    uint flags = *pointer;
                    // flags: 
                    //     [0] (1b): value
                    //     [1] (1b): continue search => empty = 00, (last) value = 01, deleted = 10, value (and not last) = 11
                    //     [2] (6b): first 6 hash bits
                    if ((flags & 3u) == 0)
                    {
                        return -(((hashIndex + i) % Capacity) + 1);
                    }
                    if ((flags | 2u) == hashMap && Helper.Cmp(pointer + 1, key, KeySize))
                    {
                        TValue val;
                        if (ValueSerializer != null) val = ValueSerializer.Read(pointer + KeySize + 1);
                        else val = GenericPointerHelper.Read<TValue>(pointer);
                        if (value.Equals(val))
                            return hashIndex + i % Capacity;
                    }
                    pointer += EntrySize;
                }
                if (diffPlus == -1)
                    diffPlus = (int)((hash * 41) % Capacity) | 1;
                clusterIndex = (clusterIndex + diffPlus) % (Capacity / ClusterSize);
                hashIndex = clusterIndex * ClusterSize + clusterOffset;
            }
            while (true);
        }

        protected void SetEntry(byte* ptr, uint keyHash, byte* key, TValue value)
        {
            *ptr = (byte)((keyHash & ~3) | 1);
            ptr++;
            GenericPointerHelper.CopyBytes(key, ptr, (uint)KeySize);
            if (ValueSerializer != null) ValueSerializer.Save(ptr + KeySize, value);
            else GenericPointerHelper.Write(ptr + KeySize, value);
        }

        public TValue FirstOrDefault(TKey key)
        {
            TValue value;
            TryGetValue(key, out value);
            return value;
        }

        public bool TryGetValue(TKey key, out TValue value)
        {
            var buffer = readKeyBufferManager.GetBuffer();
            fixed (byte* kptr = buffer)
            {
                WriteKey(kptr, key);
                var hash = Helper.MurMur.Hash(kptr, KeySize, Seed);
                byte* pointer;
                bool ret;
                if (ret = FindEntry(kptr, hash, false, out pointer) >= 0)
                {
                    Debug.Assert(ReadKey(pointer + 1).Equals(key), "found entry key is not equal to the searched one");
                    value = ReadValue(pointer + 1 + KeySize);
                }
                else
                {
                    value = default(TValue);
                }
                readKeyBufferManager.ReleaseBuffer(buffer);
                return ret;
            }
        }

        /// <summary>
        /// Sets the value
        /// </summary>
        /// <returns>if something was replaced</returns>
        public bool Set(TKey key, TValue value, bool allowOverwrite = true)
        {
            fixed (byte* kptr = setKeyBuffer)
            {
                if (KeySerializer != null)
                    KeySerializer.Save(kptr, key);
                else GenericPointerHelper.Write(kptr, key);
                var hash = Helper.MurMur.Hash(kptr, KeySize, Seed);
                byte* pointer;
                var index = FindEntry(kptr, hash, true, out pointer);

                Debug.Assert(index < 0 || ReadKey(pointer + 1).Equals(key));

                if (index < 0)
                {
                    count++;
                }
                else if (AllowDuplicates)
                {
                    Debug.Assert((*pointer & 2) == 0, "FindEntry should find last value in chain BUT it has continue flag");
                    // mark as with value and continue
                    *pointer |= 2;
                    index = FindNextEntry(kptr, hash, ref pointer);
                    Debug.Assert(index < 0, "FindEntry should find last value in chain BUT the next one exist");
                    count++;
                }
                else if (!allowOverwrite) throw new ArgumentException("An item with the same key already exists");

                SetEntry(pointer, hash, kptr, value);

                // replaced if FindEntry returned positive index
                return index >= 0;
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

        public int CopyTo(TKey[] keys, TValue[] values)
        {
            var ptr = StartPointer;
            int i = 0;
            for (int hIndex = 0; hIndex < Capacity & (keys != null | values != null); hIndex++)
            {
                if ((*ptr & 1) == 1)
                {
                    TKey key;
                    TValue value;
                    ReadEntry(ptr, out key, out value);
                    if (values != null)
                    {
                        values[i] = value;
                        if (values.Length == i + 1) values = null;
                    }
                    if (keys != null)
                    {
                        keys[i] = key;
                        if (keys.Length == i + 1) keys = null;
                    }
                    i++;
                }
                ptr++;
            }
            return i;
        }

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
                return false;
            }
        }

        public bool Contains(TKey key)
        {
            var buffer = readKeyBufferManager.GetBuffer();
            fixed (byte* kptr = buffer)
            {
                if (KeySerializer != null)
                    KeySerializer.Save(kptr, key);
                else
                    GenericPointerHelper.Write(kptr, key);
                var hash = Helper.MurMur.Hash(kptr, KeySize, Seed);
                byte* pointer;
                var index = FindEntry(kptr, hash, false, out pointer);
                readKeyBufferManager.ReleaseBuffer(buffer);
                return index >= 0;
            }
        }

        public void Add(TKey key, TValue value)
        {
            Set(key, value, false);
        }

        public bool RemoveFirst(TKey key)
        {
            fixed (byte* kptr = setKeyBuffer)
            {
                if (KeySerializer != null) KeySerializer.Save(kptr, key);
                else GenericPointerHelper.Write(kptr, key);
                var hash = Helper.MurMur.Hash(kptr, KeySize, Seed);
                byte* pointer;
                if (FindEntry(kptr, hash, false, out pointer) >= 0)
                {
                    Debug.Assert(ReadKey(pointer + 1).Equals(key), "found entry key is not equal to the searched one");
                    Remove(pointer);
                    count--;
                    return true;
                }
            }
            return false;
        }

        public int RemoveAll(TKey key)
        {
            fixed (byte* kptr = setKeyBuffer)
            {
                if (KeySerializer != null) KeySerializer.Save(kptr, key);
                else GenericPointerHelper.Write(kptr, key);
                var hash = Helper.MurMur.Hash(kptr, KeySize, Seed);

                byte* pointer;
                if (FindEntry(kptr, hash, true, out pointer) >= 0)
                {
                    byte flags = *pointer;
                    Debug.Assert(ReadKey(pointer + 1).Equals(key), "found entry key is not equal to the searched one");
                    Debug.Assert(flags != 0);
                    Remove(pointer);
                    var c = 1;
                    while ((flags & 2) != 0)
                    {
                        var index = FindNextEntry(kptr, hash, ref pointer);
                        Debug.Assert(index >= 0);
                        Debug.Assert(ReadKey(pointer + 1).Equals(key), "found entry key is not equal to the searched one");
                        flags = *pointer;
                        Remove(pointer);
                        c++;
                    }
                    count -= c;
                    return c;
                }
            }
            return 0;
        }

        public bool RemoveFirst(TKey key, TValue value)
        {
            fixed (byte* kptr = setKeyBuffer)
            {
                if (KeySerializer != null) KeySerializer.Save(kptr, key);
                else GenericPointerHelper.Write(kptr, key);
                var hash = Helper.MurMur.Hash(kptr, KeySize, Seed);
                byte* pointer;
                if (FindEntry(kptr, hash, value, out pointer) >= 0)
                {
                    Debug.Assert(ReadKey(pointer + 1).Equals(key), "found entry key is not equal to the searched one");
                    Debug.Assert(ReadValue(pointer + 1 + KeySize).Equals(value), "found entry value is not equal to the searched one");
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
            // TODO: clean only flags ?
            GenericPointerHelper.InitMemory(StartPointer, (uint)(Capacity * EntrySize), 0);
            count = 0;
        }

        public bool Contains(TKey key, TValue value)
        {
            var buffer = readKeyBufferManager.GetBuffer();
            fixed (byte* kptr = buffer)
            {
                if (KeySerializer != null)
                    KeySerializer.Save(kptr, key);
                else
                    GenericPointerHelper.Write(kptr, key);
                var hash = Helper.MurMur.Hash(kptr, KeySize, Seed);
                byte* pointer;
                var index = FindEntry(kptr, hash, value, out pointer);
                readKeyBufferManager.ReleaseBuffer(buffer);
                return index >= 0;
            }
        }

        public int CopyTo(KeyValuePair<TKey, TValue>[] array, int arrayIndex)
        {
            var ptr = StartPointer;
            int i = arrayIndex;
            for (int hIndex = 0; hIndex < Capacity && i < array.Length; hIndex++)
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
            return i;
        }

        public bool Recount()
        {
            int count = 0;
            byte* pointer = StartPointer;
            for (int i = 0; i < Capacity; i++)
            {
                count += *pointer & 1;
                pointer += EntrySize;
            }
            if (this.count != count)
            {
                this.count = count;
                return false;
            }
            return true;
        }

        public BitArray GetBlockUsageBitmap()
        {
            var bm = new BitArray(checked((int)Capacity));
            byte* pointer = StartPointer;
            for (int i = 0; i < Capacity; i++)
            {
                bm.Set(i, *pointer != 0);
                pointer += EntrySize;
            }
            return bm;
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

        [Flags]
        enum EntryFlags : byte
        {
            Empty = 0,
            Value = 1,
            ContinueSearch = 2,
            ValueAndContinue = 3
        }

        ~PageHashTableBase()
        {
            Dispose(false);
        }

        protected struct SimpleBufferManager
        {
            // TODO: multiple buffers
            public readonly int Length;
            byte[] buffer;

            public SimpleBufferManager(int length)
            {
                this.Length = length;
                this.buffer = null;
            }

            public byte[] GetBuffer()
            {
                return Interlocked.Exchange(ref buffer, null) ?? new byte[Length];
            }

            public void ReleaseBuffer(byte[] buffer)
            {
                // set only if null. To not keep new object to next GC generation
                Interlocked.CompareExchange(ref this.buffer, buffer, null);
            }
        }
    }

    public unsafe class PageHashTable<TKey, TValue> : PageHashTableBase<TKey, TValue>, IDictionary<TKey, TValue>
    {
        public PageHashTable(long capacity,
            IPageSerializer<TKey> keySerializer,
            IPageSerializer<TValue> valueSerializer,
            byte* startPointer = null,
            int clusterSize = 16)
            : base(capacity, keySerializer, valueSerializer, startPointer, clusterSize, false)
        { }

        public TValue this[TKey key]
        {
            get
            {
                TValue value;
                if (TryGetValue(key, out value))
                    return value;
                throw new KeyNotFoundException("key not found");
            }

            set
            {
                Set(key, value, true);
            }
        }

        public bool Contains(KeyValuePair<TKey, TValue> item)
        {
            TValue value;
            return TryGetValue(item.Key, out value) && item.Value.Equals(value);
        }

        public bool ContainsKey(TKey key)
        {
            return base.Contains(key);
        }

        public bool Remove(TKey key)
        {
            return RemoveFirst(key);
        }

        public bool Remove(KeyValuePair<TKey, TValue> item)
        {
            if (Contains(item))
                return Remove(item.Key);
            return false;
        }

        void ICollection<KeyValuePair<TKey, TValue>>.CopyTo(KeyValuePair<TKey, TValue>[] array, int arrayIndex)
        {
            this.CopyTo(array, arrayIndex);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }

    public unsafe class PageMultiValueHashTable<TKey, TValue> : PageHashTableBase<TKey, TValue>, ILookup<TKey, TValue>
    {
        public PageMultiValueHashTable(
            long capacity,
            IPageSerializer<TKey> keySerializer,
            IPageSerializer<TValue> valueSerializer,
            byte* startPointer = null,
            int clusterSize = 32,
            int count = 0)
            : base(capacity, keySerializer, valueSerializer, startPointer, clusterSize, true, count)
        { }

        public IEnumerable<TValue> this[TKey key]
        {
            get
            {
                return new Grouping(key, this);
            }
        }

        IEnumerator<IGrouping<TKey, TValue>> IEnumerable<IGrouping<TKey, TValue>>.GetEnumerator()
        {
            // TODO: performance :)
            var array = new KeyValuePair<TKey, TValue>[count];
            CopyTo(array, 0);
            var group = array.GroupBy(a => a.Key, a => a.Value);
            return group.GetEnumerator();
        }

        public TValue[] GetValues(TKey key, int maxsize)
        {
            //var keyBuffer = new byte[KeySize];
            //fixed (byte* kptr = keyBuffer)
            //{
            //    if (KeySerializer == null) GenericPointerHelper.Write(kptr, key);
            //    else KeySerializer.Save(kptr, key);

            //}
            return this[key].ToArray();
        }

        struct KeyEnumerator : IEnumerator<TValue>
        {
            private PageMultiValueHashTable<TKey, TValue> hashtable;
            private byte* pointer;
            private byte[] key;
            private uint keyHash;

            public TValue Current { get; private set; }

            object IEnumerator.Current
            {
                get
                {
                    return Current;
                }
            }

            public void Dispose()
            {
                key = null;
                hashtable = null;
            }

            public bool MoveNext()
            {
                if (pointer == null)
                {
                    Reset();
                    return pointer != null;
                }
                long index;
                fixed (byte* kptr = key)
                {
                    index = hashtable.FindNextEntry(kptr, keyHash, ref pointer);
                }

                if (index >= 0)
                {
                    if (hashtable.ValueSerializer != null) Current = hashtable.ValueSerializer.Read(pointer + 1 + hashtable.KeySize);
                    else Current = GenericPointerHelper.Read<TValue>(pointer + 1 + hashtable.KeySize);
                    return true;
                }
                return false;
            }

            public void Reset()
            {
                fixed (byte* kptr = key)
                {
                    hashtable.FindEntry(kptr, keyHash, false, out pointer);
                    Current = hashtable.ReadValue(pointer + 1 + hashtable.KeySize);
                }
            }

            public KeyEnumerator(PageMultiValueHashTable<TKey, TValue> hashtable, byte[] key, uint keyHash)
            {
                this.hashtable = hashtable;
                this.key = key;
                this.keyHash = keyHash;
                this.pointer = null;
                this.Current = default(TValue);
            }
        }
        // TODO: measure struct performance
        struct Grouping : IGrouping<TKey, TValue>
        {
            private PageMultiValueHashTable<TKey, TValue> hashtable;
            public TKey Key { get; }

            public Grouping(TKey key, PageMultiValueHashTable<TKey, TValue> hashtable)
            {
                this.Key = key;
                this.hashtable = hashtable;
            }

            public IEnumerator<TValue> GetEnumerator()
            {
                // TODO: cache buffer
                var keyBuffer = new byte[hashtable.KeySize];
                uint hash;
                fixed (byte* kptr = keyBuffer)
                {
                    hashtable.WriteKey(kptr, Key);
                    hash = Helper.MurMur.Hash(kptr, hashtable.KeySize, hashtable.Seed);
                }
                return new KeyEnumerator(hashtable, keyBuffer, hash);
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }
        }
    }
    public unsafe interface IPageSerializer<T>
    {
        int Size { get; }
        T Read(byte* ptr);
        void Save(byte* ptr, T value);
    }
    public unsafe static class PageHashTableHelper
    {
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
                int length = 0;
                while (length < size)
                {
                    if (*(ptr + length) == 0) break;
                    length++;
                }
                return new string((sbyte*)ptr, 0, length, StringEncoding);
            }

            public void Save(byte* ptr, string value)
            {
                fixed (char* strptr = value)
                {
                    var count = StringEncoding.GetBytes(strptr, value.Length, ptr, Size);
                    if (count < size) *(ptr + count) = 0;
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

        public static PageMultiValueHashTable<TKey, TValue> CreateStructStructMulti<TKey, TValue>(int count, byte* startPointer = null)
            where TValue : struct
            where TKey : struct
        {
            return new PageMultiValueHashTable<TKey, TValue>(count,
                //new StructPageSerializer<TKey>(), new StructPageSerializer<TValue>(), 
                null, null,
                startPointer);
        }

        public static int GetEntrySize<TKey, TValue>(IPageSerializer<TKey> key, IPageSerializer<TValue> value)
        {
            return 1 + ((key == null) ? GenericPointerHelper.SizeOf<TKey>() : key.Size)
                + ((value == null) ? GenericPointerHelper.SizeOf<TValue>() : value.Size);
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
