using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;
using System.IO;
using RaptorDB.Common;
using System.Collections.Concurrent;

namespace RaptorDB
{
    #region [ internal classes ]

    internal struct PageInfo  // FEATURE : change back to class for count access for query caching
    {
        public PageInfo(int pagenum, int uniquecount, int duplicatecount)
        {
            PageNumber = pagenum;
            UniqueCount = uniquecount;
        }
        public int PageNumber;
        public int UniqueCount;
    }

    internal struct KeyInfo
    {
        public KeyInfo(int recnum)
        {
            RecordNumber = recnum;
            DuplicateBitmapNumber = -1;
        }
        public KeyInfo(int recnum, int bitmaprec)
        {
            RecordNumber = recnum;
            DuplicateBitmapNumber = bitmaprec;
        }
        public int RecordNumber;
        public int DuplicateBitmapNumber;
    }

    internal class Page<T>
    {
        public Page() // kludge so the compiler doesn't complain
        {
            DiskPageNumber = -1;
            RightPageNumber = -1;
            tree = new SafeDictionary<T, KeyInfo>(Global.PageItemCount);
            isDirty = false;
            FirstKey = default(T);
            rwlock = new ReaderWriterLockSlim(LockRecursionPolicy.SupportsRecursion);
        }
        public int DiskPageNumber;
        public int RightPageNumber;
        public T FirstKey;
        public bool isDirty;
        public SafeDictionary<T, KeyInfo> tree;
        public List<int> allocblocks;
        public ReaderWriterLockSlim rwlock;
    }

    #endregion

    internal class MGIndex<T> where T : IComparable<T>
    {
        ILog _log = LogManager.GetLogger(typeof(MGIndex<T>));
        private SortedList<T, PageInfo> _pageList = new SortedList<T, PageInfo>();
        private ConcurrentDictionary<int, Page<T>> _cache = new ConcurrentDictionary<int, Page<T>>();
        //private SafeDictionary<int, CacheTimeOut> _usage = new SafeDictionary<int, CacheTimeOut>();
        private List<int> _pageListDiskPages = new List<int>();
        private IndexFile<T> _index;
        private bool _AllowDuplicates = true;
        private int _LastIndexedRecordNumber = 0;
        private int _maxPageItems = 0;
        Func<T, T, int> _compFunc = null;

        /// <summary>
        /// lock read when reading anything and lock write if writing to pagelist and creating new pages
        /// </summary>
        private ReaderWriterLockSlim _listLock = new ReaderWriterLockSlim(LockRecursionPolicy.SupportsRecursion);

        public MGIndex(string path, string filename, byte keysize, ushort maxcount, bool allowdups)
        {
            _AllowDuplicates = allowdups;
            _index = new IndexFile<T>(path + Path.DirectorySeparatorChar + filename, keysize, maxcount);
            _maxPageItems = maxcount;
            // load page list
            _index.GetPageList(_pageListDiskPages, _pageList, out _LastIndexedRecordNumber);
            if (_pageList.Count == 0)
            {
                Page<T> page = new Page<T>();
                page.FirstKey = (T)RDBDataType<T>.GetEmpty();
                page.DiskPageNumber = _index.GetNewPageNumber();
                page.isDirty = true;
                _pageList.Add(page.FirstKey, new PageInfo(page.DiskPageNumber, 0, 0));
                _cache.TryAdd(page.DiskPageNumber, page);
            }
            if (typeof(T) == typeof(string))
            {
                _compFunc = (Func<T, T, int>)(Delegate)(Func<string, string, int>)CultureInfo.CurrentCulture.CompareInfo.Compare;
        }
        }

        public int GetLastIndexedRecordNumber()
        {
            return _LastIndexedRecordNumber;
        }

        public WAHBitArray Query(T from, T to, int maxsize)
        {           
            // TODO : add BETWEEN code here
            T temp = default(T);
            if (from.CompareTo(to) > 0) // check values order
            {
                temp = from;
                from = to;
                to = temp;
            }
            // find first page and do > than
            bool found = false;
            int startpos = FindPageOrLowerPosition(from, ref found);

            // find last page and do < than
            int endpos = FindPageOrLowerPosition(to, ref found);

            // do all pages in between

            throw new NotImplementedException();
            //TODO: WTF ???
            return new WAHBitArray();
        }

        public WAHBitArray Query(RDBExpression exp, T from, int maxsize)
        {
            T key = from;
            if (exp == RDBExpression.Equal || exp == RDBExpression.NotEqual)
                return doEqualOp(exp, key, maxsize);

            // TODO : optimize complement search if page count less for the complement pages
            //bool found = false;
            //int last = _pageList.Count - 1;
            //int pos = FindPageOrLowerPosition(key, ref found);

            if (exp == RDBExpression.Less || exp == RDBExpression.LessEqual)
            {
                //long c = (pos+1) * _maxPageItems * 70 / 100; // 70% full pages
                //long inv = maxsize - c;
                //if (c < inv)
                    return doLessOp(exp, key);
                //else
                //{

                //}
            }
            else if (exp == RDBExpression.Greater || exp == RDBExpression.GreaterEqual)
            {
                return doMoreOp(exp, key);
            }

            return new WAHBitArray(); // blank results 
        }

        private object _setlock = new object();
        public void Set(T key, int val)
        {
            Page<T> page = null;
            using (_listLock.Reading())
            {
                PageInfo pi;
                page = LoadPage(key, out pi);

                using (page.rwlock.Writing())
                {
                KeyInfo ki;
                if (page.tree.TryGetValue(key, out ki))
                {
                    // item exists
                    if (_AllowDuplicates)
                    {
                        SaveDuplicate(key, ref ki);
                        // set current record in the bitmap also
                        _index.SetBitmapDuplicate(ki.DuplicateBitmapNumber, val);
                    }
                    ki.RecordNumber = val;
                    page.tree[key] = ki; // structs need resetting
                }
                else
                {
                    // new item 
                    ki = new KeyInfo(val);
                    if (_AllowDuplicates)
                        SaveDuplicate(key, ref ki);
                    pi.UniqueCount++;
                    page.tree.Add(key, ki);
                }

                _LastIndexedRecordNumber = val;
                page.isDirty = true;
            }
        }
            var c = page.tree.Count;
            if (c > Global.PageItemCount || (c > Global.EarlyPageSplitSize && _pageList.Count <= Global.EarlyPageCount))
                SplitPage(page.DiskPageNumber);
        }

        public bool Get(T key, out int val)
        {
            using (_listLock.Reading())
            {
            val = -1;
            PageInfo pi;
            Page<T> page = LoadPage(key, out pi);
            KeyInfo ki;
            bool ret = page.tree.TryGetValue(key, out ki);
            if (ret)
                val = ki.RecordNumber;
            return ret;
        }
        }

        public void SaveIndex()
        {
            using (_listLock.Reading())
            {
            _log.Debug("Total split time (s) = " + _totalsplits);
            _log.Debug("Total pages = " + _pageList.Count);
                var keys = _cache.Keys.ToArray();
            Array.Sort(keys);
            // save index to disk
            foreach (var i in keys)
            {
                var p = _cache[i];
                if (p.isDirty)
                {
                    _index.SavePage(p);
                    p.isDirty = false;
                }
            }
            _index.SavePageList(_pageList, _pageListDiskPages);
            _index.BitmapFlush();
        }
        }

        public void Shutdown()
        {
            using (_listLock.Writing())
            {
            SaveIndex();
            // save page list
            _index.SavePageList(_pageList, _pageListDiskPages);
            // shutdown
            _index.Shutdown();
        }
        }

        public void FreeMemory()
        {
            _index.FreeMemory();
            try
            {
                List<int> free = new List<int>();
                foreach (var c in _cache)
                {
                    if (c.Value.isDirty == false)
                        free.Add(c.Key);
                }
                _log.Debug("releasing page count = " + free.Count + " out of " + _cache.Count);
                Page<T> p;
                foreach (var i in free)
                {
                    _cache.TryRemove(i, out p);
                    p.rwlock.Dispose();
            }
            }
            catch { }
        }


        public IEnumerable<int> GetDuplicates(T key)
        {
            using (_listLock.Reading())
            {
            PageInfo pi;
            Page<T> page = LoadPage(key, out pi);
            KeyInfo ki;
            bool ret = page.tree.TryGetValue(key, out ki);
            if (ret)
                // get duplicates
                if (ki.DuplicateBitmapNumber != -1)
                    return _index.GetDuplicatesRecordNumbers(ki.DuplicateBitmapNumber);
            }
            return new List<int>();
        }

        public void SaveLastRecordNumber(int recnum)
        {
            _index.SaveLastRecordNumber(recnum);
        }

        public bool RemoveKey(T key)
        {
            using (_listLock.Reading())
            {
            PageInfo pi;
            Page<T> page = LoadPage(key, out pi);
            bool b = page.tree.Remove(key);
                using (page.rwlock.Writing())
                {
            // FIX : reset the first key for page
            if (b)
            {
                        Interlocked.Decrement(ref pi.UniqueCount);
                // FEATURE : decrease dup count
            }
            page.isDirty = true;
                }
            return b;
        }
        }

        #region [  P R I V A T E  ]
        private WAHBitArray doMoreOp(RDBExpression exp, T key)
        {
            using (_listLock.Reading())
            {
            bool found = false;
            int pos = FindPageOrLowerPosition(key, ref found);
            WAHBitArray result = new WAHBitArray();
            if (pos < _pageList.Count)
            {
                // all the pages after
                for (int i = pos + 1; i < _pageList.Count; i++)
                    doPageOperation(ref result, i);
            }
            // key page
                Page<T> page = LoadPage(_pageList.Values[pos].PageNumber);
                using (page.rwlock.Reading())
                {
            T[] keys = page.tree.Keys();
            Array.Sort(keys);

            // find better start position rather than 0
            pos = Array.IndexOf<T>(keys, key);
            if (pos == -1) pos = 0;

            for (int i = pos; i < keys.Length; i++)
            {
                T k = keys[i];
                int bn = page.tree[k].DuplicateBitmapNumber;

                if (k.CompareTo(key) > 0)
                    result = result.Or(_index.GetDuplicateBitmap(bn));

                if (exp == RDBExpression.GreaterEqual && k.CompareTo(key) == 0)
                    result = result.Or(_index.GetDuplicateBitmap(bn));
            }
                }
            return result;
        }
        }

        private WAHBitArray doLessOp(RDBExpression exp, T key)
        {
            using (_listLock.Reading())
            {
            bool found = false;
            int pos = FindPageOrLowerPosition(key, ref found);
            WAHBitArray result = new WAHBitArray();
            if (pos > 0)
            {
                // all the pages before
                for (int i = 0; i < pos - 1; i++)
                    doPageOperation(ref result, i);
            }
            // key page
                Page<T> page = LoadPage(_pageList.Values[pos].PageNumber);
                using (page.rwlock.Reading())
                {
            T[] keys = page.tree.Keys();
            Array.Sort(keys);
            for (int i = 0; i < keys.Length; i++)
            {
                T k = keys[i];
                if (k.CompareTo(key) > 0)
                    break;
                int bn = page.tree[k].DuplicateBitmapNumber;

                if (k.CompareTo(key) < 0)
                    result = result.Or(_index.GetDuplicateBitmap(bn));

                if (exp == RDBExpression.LessEqual && k.CompareTo(key) == 0)
                    result = result.Or(_index.GetDuplicateBitmap(bn));
            }
                }
            return result;
        }
        }

        private WAHBitArray doEqualOp(RDBExpression exp, T key, int maxsize)
        {
            using (_listLock.Reading())
            {
            PageInfo pi;
            Page<T> page = LoadPage(key, out pi);
            KeyInfo k;
            if (page.tree.TryGetValue(key, out k))
            {
                int bn = k.DuplicateBitmapNumber;

                if (exp == RDBExpression.Equal)
                    return _index.GetDuplicateBitmap(bn);
                else
                    return _index.GetDuplicateBitmap(bn).Not(maxsize); 
            }
            else
                return new WAHBitArray();
        }
        }

        private void doPageOperation(ref WAHBitArray res, int pageidx)
        {
            Page<T> page = LoadPage(_pageList.Values[pageidx].PageNumber);
            using (page.rwlock.Reading())
            {
            T[] keys = page.tree.Keys(); // avoid sync issues
            foreach (var k in keys)
            {
                int bn = page.tree[k].DuplicateBitmapNumber;

                res = res.Or(_index.GetDuplicateBitmap(bn));
            }
        }
        }

        private double _totalsplits = 0;
        private void SplitPage(int num)
        {
            Page<T> page = null;
            if (_pageList.Count == 1 && _listLock.WaitingWriteCount > 0)
                // some other thread is waiting to change the first page
                return;
            using (_listLock.Writing())
            {
                page = LoadPage(num);
                if (page.tree.Count < Global.PageItemCount && (page.tree.Count < Global.EarlyPageSplitSize || _pageList.Count > Global.EarlyPageCount)) return;

                using (page.rwlock.Writing())
        {
                    if (page.tree.Count < Global.PageItemCount && (page.tree.Count < Global.EarlyPageSplitSize || _pageList.Count > Global.EarlyPageCount)) return;

            // split the page
            DateTime dt = FastDateTime.Now;

            Page<T> newpage = new Page<T>();
            newpage.DiskPageNumber = _index.GetNewPageNumber();
            newpage.RightPageNumber = page.RightPageNumber;
            newpage.isDirty = true;
            page.RightPageNumber = newpage.DiskPageNumber;
            // get and sort keys
            T[] keys = page.tree.Keys();
            Array.Sort<T>(keys);
            // copy data to new 
            for (int i = keys.Length / 2; i < keys.Length; i++)
            {
                newpage.tree.Add(keys[i], page.tree[keys[i]]);
                // remove from old page
                page.tree.Remove(keys[i]);
            }
            // set the first key
            newpage.FirstKey = keys[keys.Length / 2];
            // set the first key refs
            _pageList.Remove(page.FirstKey);
            _pageList.Remove(keys[0]);
            // dup counts
            _pageList.Add(keys[0], new PageInfo(page.DiskPageNumber, page.tree.Count, 0));
            page.FirstKey = keys[0];

            // FEATURE : dup counts
            _pageList.Add(newpage.FirstKey, new PageInfo(newpage.DiskPageNumber, newpage.tree.Count, 0));
                    _cache.TryAdd(newpage.DiskPageNumber, newpage);

                    _totalsplits += FastDateTime.Now.Subtract(dt).TotalMilliseconds;
                }

            }
        }

        private Page<T> LoadPage(T key, out PageInfo pageinfo)
        {
            if (!_listLock.IsReadLockHeld) throw new InvalidOperationException("readlock not held");

            int pagenum = -1;
            // find page in list of pages

            bool found = false;
            int pos = 0;
            if (key != null)
                pos = FindPageOrLowerPosition(key, ref found);
            pageinfo = _pageList.Values[pos];
            pagenum = pageinfo.PageNumber;

            Page<T> page;
            if (_cache.TryGetValue(pagenum, out page) == false)
            {
                //load page from disk
                page = _index.LoadPageFromPageNumber(pagenum);
                _cache.TryAdd(pagenum, page);
            }
            return page;
        }

        private Page<T> LoadPage(int pagenum)
        {
            if (!(_listLock.IsReadLockHeld || _listLock.IsUpgradeableReadLockHeld || _listLock.IsWriteLockHeld))
                throw new InvalidOperationException("readlock not held");

            // page usage data 
            //_usage.Add(pagenum, new CacheTimeOut(pagenum, FastDateTime.Now.Ticks));
            return _cache.GetOrAdd(pagenum, _index.LoadPageFromPageNumber);
        }

        private void SaveDuplicate(T key, ref KeyInfo ki)
        {
            if (ki.DuplicateBitmapNumber == -1)
                ki.DuplicateBitmapNumber = _index.GetBitmapDuplaicateFreeRecordNumber();

            _index.SetBitmapDuplicate(ki.DuplicateBitmapNumber, ki.RecordNumber);
        }

        private int FindPageOrLowerPosition(T key, ref bool found)
        {
            if (_pageList.Count <= 1)
                return 0;
            // binary search
            int first = 0;
            int last = _pageList.Count - 1;
            int mid = 0;
            while (first < last)
            {
                // int divide and ceil
                mid = ((first + last - 1) >> 1) + 1;
                T k = _pageList.Keys[mid];
                int compare = _compFunc == null ? k.CompareTo(key) : _compFunc(k, key);
                if (compare < 0)
                {
                    first = mid;
                }
                if (compare == 0)
                {
                    found = true;
                    return mid;
                }
                if (compare > 0)
                {
                    last = mid - 1;
                }
            }

            return first;
        }
        #endregion

        internal object[] GetKeys()
        {
            using (_listLock.Reading())
            {
            List<object> keys = new List<object>();
            for (int i = 0; i < _pageList.Count; i++)
            {
                    Page<T> page = LoadPage(_pageList.Values[i].PageNumber);
                foreach (var k in page.tree.Keys())
                    keys.Add(k);
            }
            return keys.ToArray();
        }
        }

        internal int Count()
        {
            int count = 0;
            for (int i = 0; i < _pageList.Count; i++)
            {
                Page<T> page = LoadPage(_pageList.Values[i].PageNumber);
                foreach (var k in page.tree.Keys())
                    count++;
            }
            return count;
        }
    }
}
