using GenericPointerHelpers;
using RaptorDB.Common;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace RaptorDB.Indexes
{
    public class MMIndex<TKey> : IBetweenComparisonIndex<TKey>, IUpdatableIndex<TKey>
        where TKey : IComparable<TKey>
    {
        ILog log = LogManager.GetLogger(typeof(MMIndex<TKey>));
        private MmfTableIndexFileManager<TKey, int> fileManager;
        private IndexRootFileManager<TKey> rootManager;
        private ReaderWriterLockSlim rwlock = new ReaderWriterLockSlim();
        private IIndexRoot<TKey> root;
        public readonly int PageSize;

        public bool AllowsDuplicates => true;

        public MMIndex(string path, string filename, int pageSize, IPageSerializer<TKey> keySerializer)
        {
            var filePath = Path.Combine(path, filename);
            this.PageSize = pageSize;
            fileManager = new MmfTableIndexFileManager<TKey, int>(filePath + "-idxdata-", pageSize, keySerializer);
            rootManager = new IndexRootFileManager<TKey>(filePath + ".idxroot", keySerializer);
            root = rootManager.Load();
        }

        public void Set(TKey key, int recnum)
        {
            int split = 0;
            try
            {
                rwlock.EnterReadLock();

                var tableIndex = root.GetPageIndex(key);
                var table = fileManager.GetPage(tableIndex);
                if (table.Capacity == table.Count)
                {
                    split = 2;
                }
                else
                {
                    try
                    {
                        table.rwlock.EnterWriteLock();
                        table.Set(key, recnum);
                        if (table.Count * 4 > PageSize * 3)
                            split = 1;
                    }
                    finally
                    {
                        fileManager.MarkDirty(tableIndex, table);
                        table.rwlock.ExitWriteLock();
                    }
                }
            }
            finally
            {
                rwlock.ExitReadLock();
            }
            if (split > 0)
            {
                Split(key);
                if (split > 1) Set(key, recnum);
            }
        }

        protected void Split(TKey key)
        {
            try
            {
                rwlock.EnterUpgradeableReadLock();

                var tableIndex = root.GetPageIndex(key);
                var table = fileManager.GetPage(tableIndex);
                if (table.Count * 4 <= PageSize * 3)
                    return;

                try
                {
                    table.rwlock.EnterWriteLock();
                    rwlock.EnterWriteLock();
                    if (table.Count * 4 <= PageSize * 3)
                        return;

                    SplitCore(tableIndex, table);
                }
                finally
                {
                    rwlock.ExitWriteLock();
                    table.rwlock.ExitWriteLock();
                }
            }
            finally
            {
                rwlock.ExitUpgradeableReadLock();
            }
        }

        protected void SplitCore(int tableIndex, PageMultiValueHashTable<TKey, int> table)
        {
            var buffer = new KeyValuePair<TKey, int>[table.Count];
            var count = table.CopyTo(buffer, 0);
            var pivot = SelectPivot(buffer, count);
            var table2Index = root.CreateTable(pivot);
            table.Clear();

            // TODO: release root lock here?

            var table2 = fileManager.GetPage(table2Index);
            for (int i = 0; i < count; i++)
            {
                if (buffer[i].Key.CompareTo(pivot) < 0)
                    table.Add(buffer[i]);
                else table2.Add(buffer[i]);
            }
            fileManager.MarkDirty(tableIndex, table);
            fileManager.MarkDirty(table2Index, table2);
            log.Debug($"splitted node { tableIndex } in ratio { (float)table.Count / table2.Count }");
        }

        protected TKey SelectPivot(KeyValuePair<TKey, int>[] buffer, int count)
        {
            //TKey gt;
            //if (buffer[0].Key.CompareTo(buffer[300].Key) > 0) gt = buffer[0].Key;
            //else gt = buffer[300].Key;

            //if (gt.CompareTo(buffer[600].Key) > 0) return buffer[600].Key;
            //else return gt; 
            Array.Sort(buffer, 0, count, KeyValuePairComparer<TKey, int>.DefaultInstance);
            return buffer[count / 2].Key;
        }

        public TKey[] GetKeys()
        {
            throw new NotImplementedException();
        }

        public void Set(object key, int recnum)
            => Set((TKey)key, recnum);

        public void FreeMemory()
        {
        }

        public void SaveIndex()
        {
            rootManager.Save();
        }

        public void Dispose()
        {
            // wait for pending actions
            rwlock.EnterWriteLock();
            // save root
            rootManager.Save();
            // dispose
            rootManager = null;
            fileManager.Dispose();
            fileManager = null;
            rwlock.Dispose();
        }

        public WahBitArray QueryBetween(TKey from, TKey to)
        {
            // TODO: implement between
            throw new NotImplementedException();
        }

        public WahBitArray QueryGreater(TKey key)
            => CompareCore(key, false, 1, 0, -1);

        public WahBitArray QueryGreaterEquals(TKey key)
            => CompareCore(key, true, 1, 0, -1);

        public WahBitArray QueryLess(TKey key)
            => CompareCore(key, false, -1, 0, -1);


        public WahBitArray QueryLessEquals(TKey key)
            => CompareCore(key, true, -1, 0, -1);

        private WahBitArray CompareCore(TKey key, bool eq, int goal, int skip, int take)
        {
            if (take == 0) return new WahBitArray();
            try
            {
                rwlock.EnterReadLock();
                var result = new WahBitArray();
                var topTableIndex = root.GetPageIndex(key);
                var array = GetPageContent(topTableIndex);
                for (int i = 0; i < array.Length; i++)
                {
                    int cmp = array[i].Value.CompareTo(key);
                    if (cmp == goal || (eq && cmp == 0))
                    {
                        if (skip <= 0)
                        {
                            result.Set(array[i].Value, true);
                            take--;
                        }
                        else skip--;
                        if (take == 0) return result;
                    }
                }
                SetAllTables(result, root.GetLowerPagesIndexes(topTableIndex), skip, take);
                return result;
            }
            finally
            {
                rwlock.ExitReadLock();
            }
        }
        private KeyValuePair<TKey, int>[] GetPageContent(int pageIndex)
        {
            var table = fileManager.GetPage(pageIndex);
            try
            {
                table.rwlock.EnterReadLock();
                var array = new KeyValuePair<TKey, int>[table.Count];
                table.CopyTo(array, 0);
                return array;
            }
            finally
            {
                table.rwlock.ExitReadLock();
            }
        }

        private void SetAllTables(WahBitArray result, IEnumerable<int> tables, int skip, int take)
        {
            var buffer = new int[PageSize];
            foreach (var tableIndex in tables)
            {
                var table = fileManager.GetPage(tableIndex);
                int count;
                try
                {
                    table.rwlock.EnterReadLock();
                    table.CopyTo(null, buffer);
                    count = table.Count;
                }
                finally
                {
                    table.rwlock.ExitReadLock();
                }
                for (int i = 0; i < count; i++)
                {
                    if (skip <= 0)
                    {
                        take--;
                        result.Set(buffer[i], true);
                    }
                    else skip--;
                    if (take == 0) return;
                }
            }
        }

        public WahBitArray QueryEquals(TKey key)
        {
            try
            {
                rwlock.EnterReadLock();

                var table = fileManager.GetPage(root.GetPageIndex(key));
                try
                {
                    table.rwlock.EnterReadLock();
                    return WahBitArray.FromIndexes(table[key].ToArray());
                }
                finally
                {
                    table.rwlock.ExitReadLock();
                }
            }
            finally
            {
                rwlock.ExitReadLock();
            }
        }

        public WahBitArray QueryNotEquals(TKey key)
            => QueryEquals(key).Not();

        public TResult Accept<TResult>(IIndexAcceptable<TResult> acc)
            => acc.Accept(this);

        public bool GetFirst(TKey key, out int idx)
        {
            try
            {
                rwlock.EnterReadLock();

                var table = fileManager.GetPage(root.GetPageIndex(key));
                try
                {
                    table.rwlock.EnterReadLock();
                    return table.TryGetValue(key, out idx);
                }
                finally
                {
                    table.rwlock.ExitReadLock();
                }
            }
            finally
            {
                rwlock.ExitReadLock();
            }
        }

        public bool Remove(TKey key)
        {
            try
            {
                rwlock.EnterReadLock();

                var tableIndex = root.GetPageIndex(key);
                var table = fileManager.GetPage(tableIndex);
                try
                {
                    table.rwlock.EnterWriteLock();
                    return table.RemoveAll(key) > 0;
                }
                finally
                {
                    fileManager.MarkDirty(tableIndex, table);
                    table.rwlock.ExitWriteLock();
                }
            }
            finally
            {
                rwlock.ExitReadLock();
            }
        }

        public bool Remove(TKey key, int recnum)
        {
            try
            {
                rwlock.EnterReadLock();

                var tableIndex = root.GetPageIndex(key);
                var table = fileManager.GetPage(tableIndex);
                try
                {
                    table.rwlock.EnterWriteLock();
                    return table.RemoveFirst(key, recnum);
                }
                finally
                {
                    fileManager.MarkDirty(tableIndex, table);
                    table.rwlock.ExitWriteLock();
                }
            }
            finally
            {
                rwlock.ExitReadLock();
            }
        }

        public void ReplaceFirst(TKey key, int recnum)
        {
            try
            {
                rwlock.EnterReadLock();

                var tableIndex = root.GetPageIndex(key);
                var table = fileManager.GetPage(tableIndex);
                try
                {
                    table.rwlock.EnterWriteLock();
                    table.RemoveFirst(key);
                    table.Set(key, recnum);
                }
                finally
                {
                    fileManager.MarkDirty(tableIndex, table);
                    table.rwlock.ExitWriteLock();
                }
            }
            finally
            {
                rwlock.ExitReadLock();
            }
        }

        public void Replace(TKey key, int oldNum, int newNum)
        {
            try
            {
                rwlock.EnterReadLock();

                var tableIndex = root.GetPageIndex(key);
                var table = fileManager.GetPage(tableIndex);
                try
                {
                    table.rwlock.EnterWriteLock();
                    table.RemoveFirst(key, oldNum);
                    table.Set(key, newNum);
                }
                finally
                {
                    fileManager.MarkDirty(tableIndex, table);
                    table.rwlock.ExitWriteLock();
                }
            }
            finally
            {
                rwlock.ExitReadLock();
            }
        }
    }

    public class KeyValuePairComparer<TKey, TValue> : IComparer<KeyValuePair<TKey, TValue>>
    {
        internal IComparer<TKey> keyComparer;

        public KeyValuePairComparer(IComparer<TKey> keyComparer = null)
        {
            if (keyComparer == null)
            {
                this.keyComparer = Comparer<TKey>.Default;
            }
            else
            {
                this.keyComparer = keyComparer;
            }
        }

        public int Compare(KeyValuePair<TKey, TValue> x, KeyValuePair<TKey, TValue> y)
        {
            return keyComparer.Compare(x.Key, y.Key);
        }

        public static readonly KeyValuePairComparer<TKey, TValue> DefaultInstance = new KeyValuePairComparer<TKey, TValue>();
    }
}
