using RaptorDB.Common;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace RaptorDB.Indexes
{
    public unsafe class HashIndex<TKey> : IEqualsQueryIndex<TKey>, IDisposable
    {
        private readonly string filePath;
        private MemoryMappedFile file;
        private MemoryMappedViewAccessor accessor;
        private readonly int entrySize;
        private long size;
        private PageMultiValueHashTable<TKey, int> hashtable;
        private readonly ReaderWriterLockSlim rwlock = new ReaderWriterLockSlim(LockRecursionPolicy.NoRecursion);

        public bool AllowsDuplicates => true;

        public HashIndex(string path, string filename,
            long size = 4096,
            IPageSerializer<TKey> keySerializer = null
            )
        {
            this.filePath = Path.Combine(path, filename);
            this.size = size;
            this.entrySize = PageHashTableHelper.GetEntrySize<TKey, int>(keySerializer, null);
            Load(keySerializer);
        }

        public void FreeMemory()
        {
        }
        protected int[] EqualsQuery(TKey key)
        {
            return hashtable[key].ToArray();
        }

        public void SaveIndex()
        {
            // TODO: flush memory-mapped file?
        }

        public void Set(object key, int recnum)
        {
            Set((TKey)key, recnum);
        }

        public void Dispose(bool rwlockDispose = true)
        {
            *(int*)(hashtable.StartPointer - 4) = hashtable.Count;
            accessor.SafeMemoryMappedViewHandle.ReleasePointer();
            hashtable.Dispose();
            accessor.Dispose();
            file.Dispose();
            if (rwlockDispose) rwlock.Dispose();
        }

        public void Set(TKey key, int recnum)
        {
            try
            {
                rwlock.EnterWriteLock();
                hashtable.Set(key, recnum, false);
                if (hashtable.Count * 3 > (size * 2))
                {
                    ResizeFile(size * 4);
                }
            }
            finally
            {
                rwlock.ExitWriteLock();
            }
        }

        public void ResizeFile(long size)
        {
            throw new NotSupportedException("you can't insert more than is capacity to HashIndex");
            //var keySerializer = hashtable.KeySerializer;
            //Dispose(false);
            //this.size = size;
            //Load(keySerializer);
        }

        protected void Load(IPageSerializer<TKey> keySerializer)
        {
            file = MemoryMappedFile.CreateFromFile(filePath, FileMode.OpenOrCreate, null, size * entrySize + 4);
            accessor = file.CreateViewAccessor();
            byte* pointer = null;
            accessor.SafeMemoryMappedViewHandle.AcquirePointer(ref pointer);
            var count = *(int*)pointer;
            hashtable = new PageMultiValueHashTable<TKey, int>(size, keySerializer, null, pointer + 4, 256, count < 0 ? 0 : count);
            if (count < 0) hashtable.Recount();
            *(int*)pointer = -1;
        }

        public TKey[] GetKeys()
        {
            try
            {
                rwlock.EnterReadLock();
                return hashtable.Keys.AsArray();
            }
            finally
            {
                rwlock.ExitReadLock();
            }
        }

        void IDisposable.Dispose()
        {
            Dispose();
        }

        public WahBitArray QueryEquals(TKey key)
        {
            return WahBitArray.FromIndexes(EqualsQuery(key));
        }

        public WahBitArray QueryNotEquals(TKey key)
        {
            return QueryEquals(key).Not();
        }

        public TResult Accept<TResult>(IIndexAcceptable<TResult> acc)
            => acc.Accept(this);

        public bool GetFirst(TKey key, out int idx)
        {
            return hashtable.TryGetValue(key, out idx);
        }
    }
}
