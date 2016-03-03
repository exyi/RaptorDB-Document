using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using RaptorDB.Common;
using RaptorDB.Views;

namespace RaptorDB
{
    #region [   KeyStoreString   ]
    public class KeyStoreString : IDisposable
    {
        public KeyStoreString(string filename, bool caseSensitve)
        {
            _db = new KeyStore<int>(filename, new MMIndexColumnDefinition<int>());
            _caseSensitive = caseSensitve;
        }
        bool _caseSensitive = false;

        KeyStore<int> _db;


        public void Set(string key, string val)
        {
            Set(key, Encoding.Unicode.GetBytes(val));
        }

        public void Set(string key, byte[] val)
        {
            string str = (_caseSensitive ? key : key.ToLower());
            byte[] bkey = Encoding.Unicode.GetBytes(str);
            int hc = (int)Helper.MurMur.Hash(bkey);
            MemoryStream ms = new MemoryStream();
            ms.Write(Helper.GetBytes(bkey.Length, false), 0, 4);
            ms.Write(bkey, 0, bkey.Length);
            ms.Write(val, 0, val.Length);

            _db.SetBytes(hc, ms.ToArray());
        }

        public bool Get(string key, out string val)
        {
            val = null;
            byte[] bval;
            bool b = Get(key, out bval);
            if (b)
            {
                val = Encoding.Unicode.GetString(bval);
            }
            return b;
        }

        public bool Get(string key, out byte[] val)
        {
            string str = (_caseSensitive ? key : key.ToLower());
            val = null;
            byte[] bkey = Encoding.Unicode.GetBytes(str);
            int hc = (int)Helper.MurMur.Hash(bkey);

            if (_db.GetBytes(hc, out val))
            {
                // unpack data
                byte[] g = null;
                UnpackData(val, out val, out g);

                if (!Helper.Cmp(bkey, g))
                {
                    // if data not equal check duplicates (hash conflict)
                    List<int> ints = new List<int>(_db.GetDuplicates(hc));
                    ints.Reverse();
                    foreach (int i in ints)
                    {
                        byte[] bb = _db.FetchRecordBytes(i);
                        UnpackData(bb, out val, out g);
                        if (Helper.Cmp(bkey, g))
                            return true;

                    }
                    return false;
                }
                return true;

            }
            return false;
        }

        public int Count()
        {
            return (int)_db.Count();
        }

        public int RecordCount()
        {
            return (int)_db.RecordCount();
        }

        public void SaveIndex()
        {
            _db.SaveIndex();
        }

        public void Shutdown()
        {
            _db.Shutdown();
        }

        public void Dispose()
        {
            _db.Shutdown();
        }

        private void UnpackData(byte[] buffer, out byte[] val, out byte[] key)
        {
            int len = Helper.ToInt32(buffer, 0, false);
            key = new byte[len];
            Buffer.BlockCopy(buffer, 4, key, 0, len);
            val = new byte[buffer.Length - 4 - len];
            Buffer.BlockCopy(buffer, 4 + len, val, 0, buffer.Length - 4 - len);
        }

        public string ReadData(int recnumber)
        {
            byte[] val;
            byte[] key;
            byte[] b = _db.FetchRecordBytes(recnumber);
            UnpackData(b, out val, out key);
            return Encoding.Unicode.GetString(val);
        }

        internal void FreeMemory()
        {
            _db.FreeMemory();
        }
    }
    #endregion

    #region [   KeyStoreGuid  removed ]
    //internal class KeyStoreGuid : IDisposable //, IDocStorage
    //{
    //    public KeyStoreGuid(string filename)
    //    {
    //        _db = KeyStore<int>.Open(filename, true);
    //    }

    //    KeyStore<int> _db;

    //    public void Set(Guid key, string val)
    //    {
    //        Set(key, Encoding.Unicode.GetBytes(val));
    //    }

    //    public int Set(Guid key, byte[] val)
    //    {
    //        byte[] bkey = key.ToByteArray();
    //        int hc = (int)Helper.MurMur.Hash(bkey);
    //        MemoryStream ms = new MemoryStream();
    //        ms.Write(Helper.GetBytes(bkey.Length, false), 0, 4);
    //        ms.Write(bkey, 0, bkey.Length);
    //        ms.Write(val, 0, val.Length);

    //        return _db.SetBytes(hc, ms.ToArray());
    //    }

    //    public bool Get(Guid key, out string val)
    //    {
    //        val = null;
    //        byte[] bval;
    //        bool b = Get(key, out bval);
    //        if (b)
    //        {
    //            val = Encoding.Unicode.GetString(bval);
    //        }
    //        return b;
    //    }

    //    public bool Get(Guid key, out byte[] val)
    //    {
    //        val = null;
    //        byte[] bkey = key.ToByteArray();
    //        int hc = (int)Helper.MurMur.Hash(bkey);

    //        if (_db.Get(hc, out val))
    //        {
    //            // unpack data
    //            byte[] g = null;
    //            if (UnpackData(val, out val, out g))
    //            {
    //                if (Helper.CompareMemCmp(bkey, g) != 0)
    //                {
    //                    // if data not equal check duplicates (hash conflict)
    //                    List<int> ints = new List<int>(_db.GetDuplicates(hc));
    //                    ints.Reverse();
    //                    foreach (int i in ints)
    //                    {
    //                        byte[] bb = _db.FetchRecordBytes(i);
    //                        if (UnpackData(bb, out val, out g))
    //                        {
    //                            if (Helper.CompareMemCmp(bkey, g) == 0)
    //                                return true;
    //                        }
    //                    }
    //                    return false;
    //                }
    //                return true;
    //            }
    //        }
    //        return false;
    //    }

    //    public void SaveIndex()
    //    {
    //        _db.SaveIndex();
    //    }

    //    public void Shutdown()
    //    {
    //        _db.Shutdown();
    //    }

    //    public void Dispose()
    //    {
    //        _db.Shutdown();
    //    }

    //    public byte[] FetchRecordBytes(int record)
    //    {
    //        return _db.FetchRecordBytes(record);
    //    }

    //    public int Count()
    //    {
    //        return (int)_db.Count();
    //    }

    //    public int RecordCount()
    //    {
    //        return (int)_db.RecordCount();
    //    }

    //    private bool UnpackData(byte[] buffer, out byte[] val, out byte[] key)
    //    {
    //        int len = Helper.ToInt32(buffer, 0, false);
    //        key = new byte[len];
    //        Buffer.BlockCopy(buffer, 4, key, 0, len);
    //        val = new byte[buffer.Length - 4 - len];
    //        Buffer.BlockCopy(buffer, 4 + len, val, 0, buffer.Length - 4 - len);

    //        return true;
    //    }

    //    internal byte[] Get(int recnumber, out Guid docid)
    //    {
    //        bool isdeleted = false;
    //        return Get(recnumber, out docid, out isdeleted);
    //    }

    //    public bool RemoveKey(Guid key)
    //    {
    //        byte[] bkey = key.ToByteArray();
    //        int hc = (int)Helper.MurMur.Hash(bkey);
    //        MemoryStream ms = new MemoryStream();
    //        ms.Write(Helper.GetBytes(bkey.Length, false), 0, 4);
    //        ms.Write(bkey, 0, bkey.Length);
    //        return _db.Delete(hc, ms.ToArray());
    //    }

    //    public byte[] Get(int recnumber, out Guid docid, out bool isdeleted)
    //    {
    //        docid = Guid.Empty;
    //        byte[] buffer = _db.FetchRecordBytes(recnumber, out isdeleted);
    //        if (buffer == null) return null;
    //        if (buffer.Length == 0) return null;
    //        byte[] key;
    //        byte[] val;
    //        // unpack data
    //        UnpackData(buffer, out val, out key);
    //        docid = new Guid(key);
    //        return val;
    //    }

    //    internal int CopyTo(StorageFile<int> backup, int start)
    //    {
    //        return _db.CopyTo(backup, start);
    //    }
    //}
    #endregion

    internal class KeyStore<T> : IDisposable, IDocStorage<T> where T : IComparable<T>
    {
        public KeyStore(string filename, bool AllowDuplicateKeys)
        {
            Initialize(filename, Global.DefaultStringKeySize, ViewIndexDefinitionHelpers.GetDefaultForType<T>(AllowDuplicateKeys));
        }

        public KeyStore(string fileName, IViewColumnIndexDefinition<T> indexDefinition)
        {
            Initialize(fileName, Global.DefaultStringKeySize, indexDefinition);
        }

        private ILog log = LogManager.GetLogger(typeof(KeyStore<T>));

        private string _Path = "";
        private string _FileName = "";
        private byte _MaxKeySize;
        private StorageFile<T> _archive;
        private IEqualsQueryIndex<T> _index;
        private string _datExtension = ".mgdat";
        private string _idxExtension = ".mgidx";
        private string lockFileExtension = ".kslock";
        private FileStream lockFile;
        private System.Timers.Timer _savetimer;
        private BoolIndex _deleted;

        object _savelock = new object();
        public void SaveIndex()
        {
            if (_index == null)
                return;
            lock (_savelock)
            {
                log.Debug("saving to disk");
                _index.SaveIndex();
                _deleted?.SaveIndex();
                log.Debug("index saved");
            }
        }

        public IEnumerable<int> GetDuplicates(T key)
        {
            // get duplicates from index
            return _index.QueryEquals(key).GetBitIndexes();
        }

        public byte[] FetchRecordBytes(int record)
        {
            return _archive.ReadBytes(record);
        }

        public long Count()
        {
            int c = _archive.Count();
            if(_deleted != null) c -= (int)_deleted.GetBits().CountOnes();
            return c;
        }

        public bool Get(T key, out string val)
        {
            byte[] b;
            val = null;
            bool ret = GetBytes(key, out b);
            if (ret && b != null)
            {
                val = Encoding.Unicode.GetString(b);
            }
            return ret;
        }

        public bool GetObject(T key, out object val)
        {
            int off;
            if (IndexGetFirst(key, out off))
            {
                val = _archive.ReadObject(off);
                return true;
            }
            val = null;
            return false;
        }

        public bool GetBytes(T key, out byte[] val)
        {
            int off;
            // search index
            if (IndexGetFirst(key, out off))
            {
                val = _archive.ReadBytes(off);
                return true;
            }
            val = null;
            return false;
        }

        private bool IndexGetFirst(T key, out int offset)
        {
            if (_deleted == null)
                return _index.GetFirst(key, out offset);
            else
            {
                offset = _index.QueryEquals(key).AndNot(_deleted.GetBits()).GetFirstIndex();
                return offset >= 0;
            }
        }

        public int SetString(T key, string data)
        {
            return SetBytes(key, Encoding.Unicode.GetBytes(data));
        }

        public int SetObject(T key, object doc)
        {
            int recno = (int)_archive.WriteObject(key, doc);
            // save to index
            _index.Set(key, recno);

            return recno;
        }

        public int SetBytes(T key, byte[] data)
        {
            int recno = (int)_archive.WriteData(key, data);
            // save to index
            _index.Set(key, recno);

            return recno;
        }

        private object _shutdownlock = new object();
        public void Shutdown()
        {
            lock (_shutdownlock)
            {
                if (_index != null)
                    log.Debug("Shutting down");
                else
                    return;
                _savetimer.Enabled = false;
                SaveIndex();
                lockFile.Dispose();
                File.Delete(lockFile.Name);

                if (_deleted != null)
                    _deleted.Dispose();
                if (_index != null)
                    _index.Dispose();
                if (_archive != null)
                    _archive.Shutdown();
                _index = null;
                _archive = null;
                _deleted = null;
                //log.Debug("Shutting down log");
                //LogManager.Shutdown();
            }
        }

        public void Dispose()
        {
            Shutdown();
        }

        #region [            P R I V A T E     M E T H O D S              ]

        private void Initialize(string fileName, byte maxkeysize, IViewColumnIndexDefinition indexDefinition)
        {
            _MaxKeySize = RDBDataType<T>.GetByteSize(maxkeysize);

            _Path = Path.GetDirectoryName(fileName);
            Directory.CreateDirectory(_Path);

            _FileName = Path.GetFileNameWithoutExtension(fileName);
            var wasLocked = File.Exists(fileName + lockFileExtension);
            if (wasLocked) File.Delete(fileName + lockFileExtension);
            lockFile = File.Open(fileName + lockFileExtension, FileMode.OpenOrCreate, FileAccess.Read, FileShare.None);

            //LogManager.Configure(_Path + Path.DirectorySeparatorChar + _FileName + ".txt", 500, false);

            _index = indexDefinition.CreateIndex(_Path, _FileName) as IEqualsQueryIndex<T>;
            if (_index == null) throw new NotSupportedException("specified index does not support equals queries");

            string db = _Path + Path.DirectorySeparatorChar + _FileName + _datExtension;
            _archive = new StorageFile<T>(db, Global.SaveAsBinaryJSON ? SF_FORMAT.BSON : SF_FORMAT.JSON, false);

            if (!(_index is IUpdatableIndex<T>))
                _deleted = new BoolIndex(_Path, _FileName, "_deleted.idx");

            log.Debug("Current Count = " + RecordCount().ToString("#,0"));

            if (wasLocked) RebuildIndex();

            log.Debug("Starting save timer");
            _savetimer = new System.Timers.Timer();
            _savetimer.Elapsed += new System.Timers.ElapsedEventHandler(_savetimer_Elapsed);
            _savetimer.Interval = Global.SaveIndexToDiskTimerSeconds * 1000;
            _savetimer.AutoReset = true;
            _savetimer.Start();

        }

        private void RebuildIndex()
        {
            log.Debug("Rebuilding index...");
            // check last index record and archive record
            //       rebuild index if needed
            for (int i = 0; i < _archive.Count(); i++)
            {
                bool deleted = false;
                T key = _archive.GetKey(i, out deleted);
                if (!deleted)
                    _index.Set(key, i);
                else
                    IndexRm(key);

                if (i % 100000 == 0)
                    log.Debug("100,000 items re-indexed");
            }
            log.Debug("Rebuild index done.");
        }

        private void IndexRm(T key, int recnum)
        {
            if (_deleted == null)
                ((IUpdatableIndex<T>)_index).Remove(key);
            else _deleted.Set(true, recnum);
        }

        private bool IndexRm(T key)
        {
            if (_deleted == null)
                return ((IUpdatableIndex<T>)_index).Remove(key);
            else
            {
                var q = _index.QueryEquals(key);
                _deleted.InPlaceOR(q);
                return q.Length > 0;
            }
        }

        void _savetimer_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
        {
            SaveIndex();
        }

        #endregion

        public int RecordCount()
        {
            return _archive.Count();
        }

        internal byte[] FetchRecordBytes(int record, out bool isdeleted)
        {
            StorageItem<T> meta;
            byte[] b = _archive.ReadBytes(record, out meta);
            isdeleted = meta.isDeleted;
            return b;
        }

        internal bool Delete(T id)
        {
            // write a delete record
            _archive.Delete(id);
            return IndexRm(id);
        }

        internal bool DeleteReplicated(T id)
        {
            // write a delete record for replicated object
            _archive.DeleteReplicated(id);
            return IndexRm(id);
        }

        internal int CopyTo(StorageFile<T> storagefile, long startrecord)
        {
            return _archive.CopyTo(storagefile, startrecord);
        }

        public byte[] GetBytes(int rowid, out StorageItem<T> meta)
        {
            return _archive.ReadBytes(rowid, out meta);
        }

        internal void FreeMemory()
        {
            _index.FreeMemory();
        }

        public object GetObject(int rowid, out StorageItem<T> meta)
        {
            return _archive.ReadObject(rowid, out meta);
        }

        public StorageItem<T> GetMeta(int rowid)
        {
            return _archive.ReadMeta(rowid);
        }

        internal int SetReplicationObject(T key, object doc)
        {
            int recno = (int)_archive.WriteReplicationObject(key, doc);
            // save to index
            _index.Set(key, recno);

            return recno;
        }
    }
}
