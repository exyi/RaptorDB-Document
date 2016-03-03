using GenericPointerHelpers;
using RaptorDB.Common;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RaptorDB.Indexes
{
    class IndexRootFileManager<TKey>
        where TKey : IComparable<TKey>
    {
        public readonly string FileName;
        public readonly IPageSerializer<TKey> Serializer;
        private SortedList<TKey, int> pages;
        private object locker = new object();

        public IndexRootFileManager(string fileName, IPageSerializer<TKey> serializer)
        {
            this.FileName = fileName;
            Serializer = serializer;
        }

        public unsafe IIndexRoot<TKey> Load()
        {
            if (pages == null)
                lock (locker) if (pages == null)
                    {
                        if (File.Exists(FileName))
                        {
                            var file = File.ReadAllBytes(FileName);
                            var entrySize = 4 + (Serializer == null ? GenericPointerHelper.SizeOf<TKey>() : Serializer.Size);
                            var count = file.Length / entrySize;
                            var sl = new SortedList<TKey, int>(count);
                            fixed (byte* filePointer = file)
                            {
                                var ptr = filePointer;
                                for (int i = 0; i < count; i++)
                                {
                                    var value = *(int*)ptr;
                                    var key = Serializer == null ? GenericPointerHelper.Read<TKey>(ptr + 4) : Serializer.Read(ptr + 4);

                                    sl.Add(key, value);
                                    ptr += entrySize;
                                }
                            }
                        }
                        else
                        {
                            pages = new SortedList<TKey, int>();
                            if (Serializer != null)
                            {
                                var eb = new byte[Serializer.Size];
                                fixed (byte* ebp = eb)
                                {
                                    pages.Add(Serializer.Read(ebp), 0);
                                }
                            }
                            else pages.Add(default(TKey), 0);
                        }
                    }
            return new SortedListIndexRoot(pages);
        }
        public unsafe void Save()
        {
            lock (locker)
            {
                var entrySize = 4 + (Serializer == null ? GenericPointerHelper.SizeOf<TKey>() : Serializer.Size);
                var count = pages.Count;
                var file = new byte[entrySize * count];
                fixed (byte* filePointer = file)
                {
                    var ptr = filePointer;
                    foreach (var item in pages)
                    {
                        *(int*)ptr = item.Value;
                        if (Serializer == null) GenericPointerHelper.Write(ptr + 4, item.Key);
                        else Serializer.Save(ptr + 4, item.Key);
                        ptr += entrySize;
                    }
                }
                File.WriteAllBytes(FileName, file);
            }
        }

        class SortedListIndexRoot : IIndexRoot<TKey>
        {
            private SortedList<TKey, int> pages;
            private Func<TKey, TKey, int> compFunc;

            public SortedListIndexRoot(SortedList<TKey, int> pages)
            {
                this.pages = pages;
                if (typeof(TKey) == typeof(string))
                {
                    compFunc = (Func<TKey, TKey, int>)(Delegate)(Func<string, string, int>)CultureInfo.CurrentCulture.CompareInfo.Compare;
                }
            }

            public int GetPageIndex(TKey key)
            {
                if (pages.Count <= 1)
                    return 0;
                var keys = pages.Keys;
                // binary search
                int first = 0;
                int last = pages.Count - 1;
                int mid = 0;
                while (first < last)
                {
                    // int divide and ceil
                    mid = ((first + last - 1) >> 1) + 1;
                    var k = keys[mid];
                    int compare = compFunc == null ? k.CompareTo(key) : compFunc(k, key);
                    if (compare < 0)
                    {
                        first = mid;
                    }
                    if (compare == 0)
                    {
                        return mid;
                    }
                    if (compare > 0)
                    {
                        last = mid - 1;
                    }
                }

                return first;
            }

            public IEnumerable<int> GetLowerPagesIndexes(int index)
            {
                for (int i = index - 1; i >= 0; i--)
                {
                    yield return pages.Values[i];
                }
            }

            public IEnumerable<int> GetUpperPagesIndexes(int index)
            {
                for (int i = index + 1; i < pages.Count; i++)
                {
                    yield return pages.Values[i];
                }
            }

            public int CreateTable(TKey firstKey)
            {
                var i = pages.Count;
                pages.Add(firstKey, i);
                return i;
            }
        }
    }
}
