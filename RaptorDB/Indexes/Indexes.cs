using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using RaptorDB.Common;
using System.Runtime.InteropServices;

namespace RaptorDB
{
    #region [  TypeIndexes  ]
    public class TypeIndexes<T> : MGIndex<T>, IComparisonIndex<T> where T : IComparable<T>
    {
        public TypeIndexes(string path, string filename, byte keysize)
            : base(path, filename + ".mgidx", keysize, Global.PageItemCount, true)
        {

        }
        public TypeIndexes(string path, string filename, byte keysize, bool allowDups)
            : base(path, filename + ".mgidx", keysize, Global.PageItemCount, allowDups)
        {

        }

        public void Set(object key, int recnum)
        {
            if (key == null) return; // FEATURE : index null values ??

            base.Set((T)key, recnum);
        }

        void IIndex.FreeMemory()
        {
            base.FreeMemory();
            base.SaveIndex();
        }

        public TResult Accept<TResult>(IIndexAcceptable<TResult> acc)
        {
            return acc.Accept(this);
        }
    }
    #endregion

    #region [  BoolIndex  ]
    public class BoolIndex : IEqualsQueryIndex<bool>
    {
        public BoolIndex(string path, string filename, string extension)
        {
            // create file
            _filename = filename + extension;
            _path = path;
            if (_path.EndsWith(Path.DirectorySeparatorChar.ToString()) == false)
                _path += Path.DirectorySeparatorChar.ToString();

            if (File.Exists(_path + _filename))
                ReadFile();
        }

        private WahBitArray _bits = new WahBitArray();
        private string _filename;
        private string _path;
        private object _lock = new object();

        public bool AllowsDuplicates => true;

        //private bool _inMemory = false;

        public WahBitArray GetBits()
        {
            return _bits.Copy();
        }

        public void FreeMemory()
        {
            lock (_lock)
            {
                // free memory
                //_bits.FreeMemory();
                // save to disk
                //SaveIndex();
            }
        }

        public void Dispose()
        {
            // shutdown
            //if (_inMemory == false)
            WriteFile();
        }

        public void SaveIndex()
        {
            //if (_inMemory == false)
            WriteFile();
        }

        public void InPlaceOR(WahBitArray left)
        {
            lock (_lock)
                _bits = _bits.Or(left);
        }

        private void WriteFile()
        {
            lock (_lock)
            {
                WahBitArrayState t;
                uint[] ints = _bits.GetCompressed(out t);
                MemoryStream ms = new MemoryStream();
                BinaryWriter bw = new BinaryWriter(ms);
                bw.Write((byte)t);// write new format with the data type byte
                foreach (var i in ints)
                {
                    bw.Write(i);
                }
                File.WriteAllBytes(_path + _filename, ms.ToArray());
            }
        }

        private void ReadFile()
        {
            byte[] b = File.ReadAllBytes(_path + _filename);
            WahBitArrayState t = WahBitArrayState.Wah;
            int j = 0;
            if (b.Length % 4 > 0) // new format with the data type byte
            {
                t = (WahBitArrayState)Enum.ToObject(typeof(WahBitArrayState), b[0]);
                j = 1;
            }
            List<uint> ints = new List<uint>();
            for (int i = 0; i < b.Length / 4; i++)
            {
                ints.Add((uint)Helper.ToInt32(b, (i * 4) + j));
            }
            _bits = new WahBitArray(t, ints.ToArray());
        }

        public WahBitArray QueryEquals(bool key)
        {
            if (key)
                return _bits;
            else return _bits.Not();
        }

        public WahBitArray QueryNotEquals(bool key)
        {
            return QueryEquals(!key);
        }

        public void Set(bool key, int recnum)
        {
            lock (_lock)
                _bits.Set(recnum, key);
        }

        public bool[] GetKeys()
            => new[] { true, false };

        public TResult Accept<TResult>(IIndexAcceptable<TResult> acc)
            => acc.Accept(this);

        void IIndex.Set(object key, int recnum)
            => Set((bool)key, recnum);

        public bool GetFirst(bool key, out int idx)
        {
            throw new NotImplementedException();
        }
    }
    #endregion


    internal class ObjectToStringIndex<T> : MGIndex<string>, IComparisonIndex<T>
    {
        public ObjectToStringIndex(string path, string filename, byte maxLength)
            : base(path, filename + ".mgidx", maxLength, Global.PageItemCount, true)
        {
        }

        void IIndex.FreeMemory()
        {
            base.FreeMemory();
            base.SaveIndex();
        }

        public WahBitArray QueryGreater(T key)
            => QueryGreater(key.ToString());

        public WahBitArray QueryGreaterEquals(T key)
            => QueryGreaterEquals(key.ToString());

        public WahBitArray QueryLess(T key)
            => QueryLess(key.ToString());

        public WahBitArray QueryLessEquals(T key)
            => QueryLessEquals(key.ToString());

        public WahBitArray QueryEquals(T key)
            => QueryEquals(key.ToString());

        public WahBitArray QueryNotEquals(T key)
            => QueryNotEquals(key.ToString());

        public void Set(T key, int recnum)
        {
            if (key != null)
            {
                base.Set(key.ToString(), recnum);
            }
        }

        T[] IIndex<T>.GetKeys()
        {
            throw new NotSupportedException("ObjectToStringIndex can't rebuild keys from stored strings");
        }

        public TResult Accept<TResult>(IIndexAcceptable<TResult> acc)
            => acc.Accept(this);

        public void Set(object key, int recnum)
            => Set((T)key, recnum);

        public bool GetFirst(T key, out int idx)
        {
            return base.GetFirst(key.ToString(), out idx);
        }
    }

    #region [  FullTextIndex  ]
    internal class FullTextIndex : Hoot, IContainsIndex<string>
    {
        public FullTextIndex(string IndexPath, string FileName, bool docmode, bool sortable)
            : base(IndexPath, FileName, docmode)
        {
            if (sortable)
            {
                _idx = new TypeIndexes<string>(IndexPath, FileName, Global.DefaultStringKeySize);
                _sortable = true;
            }
        }
        private bool _sortable = false;
        private IIndex<string> _idx;

        public bool AllowsDuplicates => true;

        public void Set(string key, int recnum)
        {
            base.Index(recnum, key);
            if (_sortable)
                _idx.Set(key, recnum);
        }

        public void SaveIndex()
        {
            base.Save();
            if (_sortable)
                _idx.SaveIndex();
        }

        public string[] GetKeys()
        {
            if (_sortable)
                return _idx.GetKeys(); // support get keys 
            else
                return new string[] { };
        }
        void IIndex.FreeMemory()
        {
            base.FreeMemory();

            this.SaveIndex();
        }

        public override void Dispose()
        {
            this.SaveIndex();
            base.Dispose();
            if (_sortable) _idx.Dispose();
        }

        public TResult Accept<TResult>(IIndexAcceptable<TResult> acc)
            => acc.Accept(this);

        public WahBitArray QueryContains(string value)
        {
            return base.Query(value);
        }

        public void Set(object key, int recnum)
            => Set((string)key, recnum);
    }
    #endregion

    #region [  NoIndex  ]
    internal class NoIndex : IIndex<object>
    {
        public void Set(object key, int recnum)
        {
            // ignore set
        }

        public void FreeMemory()
        {

        }

        public void Dispose()
        {

        }

        public void SaveIndex()
        {

        }

        public object[] GetKeys()
        {
            return new object[] { };
        }

        public TResult Accept<TResult>(IIndexAcceptable<TResult> acc)
            => acc.Accept(this);

        public static readonly NoIndex Instance = new NoIndex();

        public bool AllowsDuplicates => true;
    }
    #endregion
}
