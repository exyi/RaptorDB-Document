using RaptorDB.Common;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO.MemoryMappedFiles;
using System.Linq;
using System.Text;

namespace RaptorDB.Indexes
{
    public class MmfTableIndexFileManager<TKey, TValue>: IDisposable
    {
        readonly int PageSize;
        readonly int HashtableCapacity;
        readonly IPageSerializer<TKey> KeySerializer;
        readonly IPageSerializer<TValue> ValueSerializer;
        readonly ConcurrentDictionary<int, WeakReference<PageMultiValueHashTable<TKey, TValue>>> TableCache = new ConcurrentDictionary<int, WeakReference<PageMultiValueHashTable<TKey, TValue>>>();
        public readonly string FilePrefix;
        MmFileInfo[] Files;
        object initLocker = new object();
        object cacheLocker = new object();

        public MmfTableIndexFileManager(string filePrefix, int hashtableCapacity, IPageSerializer<TKey> keySerializer = null, IPageSerializer<TValue> valueSerializer = null)
        {
            this.FilePrefix = filePrefix;
            this.KeySerializer = keySerializer;
            this.ValueSerializer = valueSerializer;
            this.Files = new MmFileInfo[0];
            this.PageSize = hashtableCapacity * PageHashTableHelper.GetEntrySize(keySerializer, valueSerializer) + 4;
            this.HashtableCapacity = hashtableCapacity;
        }

        public PageMultiValueHashTable<TKey, TValue> GetPage(int index)
        {
            WeakReference<PageMultiValueHashTable<TKey, TValue>> t;
            PageMultiValueHashTable<TKey, TValue> table;
            if (TableCache.TryGetValue(index, out t))
            {
                if (t.TryGetTarget(out table))
                {
                    return table;
                }
            }
            
            lock(cacheLocker)
            {
                if (TableCache.TryRemove(index, out t) && t.TryGetTarget(out table))
                {
                    TableCache.TryAdd(index, t);
                }
                else
                {
                    table = LoadHashtable(index);
                    if (!TableCache.TryAdd(index, new WeakReference<PageMultiValueHashTable<TKey, TValue>>(table)))
                        throw new Exception("fuck!");
                }
                return table;
            }
        }

        public unsafe void MarkDirty(int index, PageMultiValueHashTable<TKey, TValue> page)
        {
            *(((int*)page.StartPointer) - 1) = page.Count;
        }

        private PageMultiValueHashTable<TKey, TValue> LoadHashtable(int index)
        {
            var fi = Helper.Log2(index + 1) - 1;
            if (fi >= Files.Length)
                InitFiles(fi);
            var file = Files[fi];
            return CreateTable(file, index);
        }

        private unsafe PageMultiValueHashTable<TKey, TValue> CreateTable(MmFileInfo file, int index)
        {
            var pointer = file.StartPointer + (PageSize * (index - file.FirstPageIndex));
            return CreateTable(pointer);
        }

        private unsafe PageMultiValueHashTable<TKey, TValue> CreateTable(byte* pointer)
        {
            return new PageMultiValueHashTable<TKey, TValue>(HashtableCapacity, KeySerializer, ValueSerializer,
                pointer + 4, 256, *(int*)pointer);
        }

        private void InitFiles(int fileIndex)
        {
            lock(initLocker)
            {
                if (Files.Length > fileIndex) return;
                var l = Files.Length;
                Array.Resize(ref Files, fileIndex + 1);
                for (int i = l; i <= fileIndex; i++)
                {
                    InitFile(fileIndex);
                }
            }
        }

        private void InitFile(int fileIndex)
        {
            var size = fileIndex == 0 ? 1 : Files[fileIndex - 1].Count * 2;
            var file = MmFileInfo.OpenOrCreate(FilePrefix + fileIndex + ".phti", size * PageSize, size - 1, size);
            Files[fileIndex] = file;
        }

        public void Dispose()
        {
            foreach (var file in Files)
            {
                file.Dispose();
            }
        }

        public unsafe class MmFileInfo: IDisposable
        {
            public readonly int FirstPageIndex;
            public readonly int Count;
            public readonly long Size;
            public readonly MemoryMappedFile File;
            public readonly MemoryMappedViewAccessor Accessor;
            public readonly byte* StartPointer;

            public MmFileInfo(int firstPageIndex, int count, long size, MemoryMappedFile file)
            {
                this.FirstPageIndex = firstPageIndex;
                this.Count = count;
                this.Size = size;
                this.File = file;
                this.Accessor = file.CreateViewAccessor();
                Accessor.SafeMemoryMappedViewHandle.AcquirePointer(ref StartPointer);
            }

            public static MmFileInfo OpenOrCreate(string filePath, long size, int firstPageIndex, int pageCount)
            {
                var mmf = MemoryMappedFile.CreateFromFile(filePath, System.IO.FileMode.OpenOrCreate, null, size);
                return new MmFileInfo(firstPageIndex, pageCount, size, mmf);
            }

            public void Dispose()
            {
                Accessor.SafeMemoryMappedViewHandle.ReleasePointer();
                Accessor.Dispose();
                File.Dispose();
            }
        }
    }
}
