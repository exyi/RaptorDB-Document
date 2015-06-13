using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Collections;
using System.Linq.Expressions;
using System.Threading.Tasks;
using System.Reflection;
using System.Reflection.Emit;
using Microsoft.CSharp;
using System.CodeDom.Compiler;
using System.ComponentModel;
using RaptorDB.Common;
using System.Threading;
using fastJSON;
using System.Runtime.InteropServices;

namespace RaptorDB.Views
{
    internal class tran_data
    {
        public Guid docid;
        public Dictionary<Guid, List<object[]>> rows;
    }

    public interface IViewHandler
    {
        int NextRowNumber();
        ViewBase View { get; }
        Type GetFireOnType();
        void FreeMemory();
        void Commit(int id);
        void RollBack(int id);
        void Insert(Guid docid, object doc);
        bool InsertTransaction(Guid docid, object doc);
        void Shutdown();
        void Delete(Guid docid);
        int Count(string filter);
        IResult Query(int start, int count);
        IResult Query(int start, int count, string orderby);
        IResult Query(string filter, int start, int count, string orderby);
        int ViewDelete(string filter);
        bool ViewInsert(Guid id, object row);

        bool IsActive { get; }
        bool BackgroundIndexing { get; }

        ViewRowDefinition GetSchema();
        void SetView(ViewBase view, IDocStorage<Guid> objStore);
    }

    internal interface IViewHandler<TSchema> : IViewHandler
    {
        Result<TSchema> Query(Expression<Predicate<TSchema>> filter, int start, int count, string orderby);
        new Result<TSchema> Query(string filter, int start, int count, string orderby);
        int Count(Expression<Predicate<TSchema>> filter);
        int ViewDelete(Expression<Predicate<TSchema>> filter);
    }

    internal class ViewHandler<TDoc, TSchema> : IViewHandler<TSchema>
    {
        private View<TDoc> _view;
        ViewBase.MapFunctionDelgate<TDoc> mapper;
        protected static readonly string _S = Path.DirectorySeparatorChar.ToString();

        protected ILog _log = LogManager.GetLogger(typeof(ViewHandler<TDoc, TSchema>));
        protected string _path;
        protected ViewManager _viewmanager;
        protected Dictionary<string, IIndex> _indexes = new Dictionary<string, IIndex>();
        protected StorageFile<Guid> _viewData;
        protected BoolIndex _deletedRows;
        protected string _docid = "docid";
        protected string[] _colnames;
        protected ViewRowDefinition _schema;
        protected SafeDictionary<int, tran_data> _transactions = new SafeDictionary<int, tran_data>();
        protected SafeDictionary<string, int> _nocase = new SafeDictionary<string, int>();
        protected Dictionary<string, byte> _idxlen = new Dictionary<string, byte>();
        RowFill<TSchema> _rowfill;

        protected System.Timers.Timer _saveTimer;

        protected Type basetype; // used for mapper

        bool _isDirty = false;
        protected bool _stsaving = false;

        protected const string _dirtyFilename = "temp.$";
        protected const int _RaptorDBVersion = 4; // used for engine changes to views
        protected const string _RaptorDBVersionFilename = "RaptorDB.version";

        public ViewBase View { get { return _view; } }
        public bool BackgroundIndexing { get { return _view.BackgroundIndexing; } }
        public bool IsActive { get { return _view.isActive; } }

        public ViewHandler(string path, ViewManager manager)
        {
            _path = path;
            _viewmanager = manager;
        }


        private object _stlock = new object();
        void _saveTimer_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
        {
            lock (_stlock)
            {
                _stsaving = true;
                foreach (var i in _indexes)
                    i.Value.SaveIndex();

                _deletedRows.SaveIndex();
                _stsaving = false;
            }
        }

        public Type GetFireOnType()
        {
            return basetype;
        }

        /// <summary>
        /// Inits directory structure, deleted index, storage and checks if rebuild
        /// </summary>
        /// <returns>rebuild</returns>
        protected bool InitStorage(string viewName, int viewVersion)
        {
            bool rebuild = false;

            if (!_path.EndsWith(_S)) _path += _S;
            _path += viewName + _S;
            if (Directory.Exists(_path) == false)
            {
                Directory.CreateDirectory(_path);
                rebuild = true;
            }
            else
            {
                // read version file and check with view
                int version = 0;
                if (File.Exists(_path + viewName + ".version"))
                {
                    int.TryParse(File.ReadAllText(_path + viewName + ".version"), out version);
                    if (version != viewVersion)
                    {
                        _log.Debug("Newer view version detected");
                        rebuild = true;
                    }
                }
            }

            if (File.Exists(_path + _dirtyFilename))
            {
                _log.Debug("Last shutdown failed, rebuilding view : " + viewName);
                rebuild = true;
            }

            if (File.Exists(_path + _RaptorDBVersionFilename))
            {
                // check view engine version
                string s = File.ReadAllText(_path + _RaptorDBVersionFilename);
                int version = 0;
                int.TryParse(s, out version);
                if (version != _RaptorDBVersion)
                {
                    _log.Debug("RaptorDB view engine upgrade, rebuilding view : " + viewName);
                    rebuild = true;
                }
            }
            else
            {
                _log.Debug("RaptorDB view engine upgrade, rebuilding view : " + viewName);
                rebuild = true;
            }

            if (rebuild)
            {
                _log.Debug("Deleting old view data folder = " + viewName);
                Directory.Delete(_path, true);
                Directory.CreateDirectory(_path);
            }


            _deletedRows = new BoolIndex(_path, viewName, ".deleted");

            _viewData = new StorageFile<Guid>(_path + viewName + ".mgdat");

            return rebuild;
        }

        void IViewHandler.SetView(ViewBase view, IDocStorage<Guid> objStore)
        {
            SetView((View<TDoc>)view, objStore);
        }
        public void SetView(View<TDoc> view, IDocStorage<Guid> docs)
        {
            this._view = view;
            view.AutoInitIndexDefinitions();
            _schema = new ViewRowDefinition()
            {
                Name = _view.Name,
                Columns = view.IndexDefinitions.ToList()
            };
            _colnames = _schema.Columns.Select(v => v.Key).Where(c => c != _docid).ToArray();
            //Array.Sort(_colnames);
            _rowfill = ViewSchemaHelper.CreateRowFiller<TSchema>(_colnames);

            var rebuild = InitStorage(view.Name, view.Version);

            CreateLoadIndexes(_schema);

            mapper = view.Mapper;

            basetype = view.GetDocType();

            if (rebuild)
                Task.Factory.StartNew(() => RebuildFromScratch(docs));

            _saveTimer = new System.Timers.Timer();
            _saveTimer.AutoReset = true;
            _saveTimer.Elapsed += new System.Timers.ElapsedEventHandler(_saveTimer_Elapsed);
            _saveTimer.Interval = Global.SaveIndexToDiskTimerSeconds * 1000;
            _saveTimer.Start();
        }

        public void FreeMemory()
        {
            _log.Debug("free memory : " + _view.Name);
            foreach (var i in _indexes)
                i.Value.FreeMemory();

            _deletedRows.FreeMemory();
            InvalidateSortCache();
        }

        public void Commit(int id)
        {
            tran_data data = null;
            // save data to indexes
            if (_transactions.TryGetValue(id, out data))
            {
                // delete any items with docid in view
                if (_view.DeleteBeforeInsert)
                    DeleteRowsWith(data.docid);
                SaveAndIndex(data.rows);
            }
            // remove in memory data
            _transactions.Remove(id);
        }

        public void RollBack(int ID)
        {
            // remove in memory data
            _transactions.Remove(ID);
        }

        void IViewHandler.Insert(Guid docid, object doc)
        {
            Insert(docid, (TDoc)doc);
        }
        public void Insert(Guid guid, TDoc doc)
        {
            // TODO: optimize (allocation)
            apimapper api = new apimapper(_viewmanager, this);

            if (basetype == doc.GetType())
            {
                if (_view.Mapper != null)
                    _view.Mapper(api, guid, doc);
            }
            else if (mapper != null)
                mapper(api, guid, doc);

            // map objects to rows
            foreach (var d in api.emitobj)
                api.emit.Add(d.Key, ViewSchemaHelper.ExtractRows(d.Value, _colnames));

            // delete any items with docid in view
            if (_view.DeleteBeforeInsert)
                DeleteRowsWith(guid);

            SaveAndIndex(api.emit);
        }

        private void SaveAndIndex(Dictionary<Guid, List<object[]>> rows)
        {
            foreach (var d in rows)
            {
                // insert new items into view
                InsertRowsWithIndexUpdate(d.Key, d.Value);
            }
            InvalidateSortCache();
        }

        bool IViewHandler.InsertTransaction(Guid docid, object doc)
        {
            return InsertTransaction(docid, (TDoc)doc);
        }

        public bool InsertTransaction(Guid docid, TDoc doc)
        {
            apimapper api = new apimapper(_viewmanager, this);
            if (basetype == doc.GetType())
            {
                var view = _view;

                try
                {
                    if (view.Mapper != null)
                        view.Mapper(api, docid, doc);
                }
                catch (Exception ex)
                {
                    _log.Error(ex);
                    return false;
                }
            }
            else if (mapper != null)
                mapper(api, docid, doc);

            if (api._RollBack == true)
                return false;

            // map emitobj -> rows
            foreach (var d in api.emitobj)
                api.emit.Add(d.Key, ViewSchemaHelper.ExtractRows(d.Value, _colnames));

            //Dictionary<Guid, List<object[]>> rows = new Dictionary<Guid, List<object[]>>();
            tran_data data;
            if (_transactions.TryGetValue(Thread.CurrentThread.ManagedThreadId, out data))
            {
                // TODO : exists -> merge data??
            }
            else
            {
                data = new tran_data();
                data.docid = docid;
                data.rows = api.emit;
                _transactions.Add(Thread.CurrentThread.ManagedThreadId, data);
            }

            return true;
        }

        // FEATURE : add query caching here
        public Result<TSchema> Query(string filter, int start, int count)
        {
            return Query(filter, start, count, null);
        }

        IResult IViewHandler.Query(string filter, int start, int count, string orderby)
        {
            return Query(filter, start, count, orderby);
        }
        public Result<TSchema> Query(string filter, int start, int count, string orderby)
        {
            return Query(ParseFilter(filter), start, count, orderby);
        }

        public Result<TSchema> Query(Expression<Predicate<TSchema>> filter, int start, int count)
        {
            return Query(filter, start, count, null);
        }

        // FEATURE : add query caching here
        public Result<TSchema> Query(Expression<Predicate<TSchema>> filter, int start, int count, string orderby)
        {
            if (filter == null)
                return Query(start, count);

            DateTime dt = FastDateTime.Now;
            _log.Debug("query : " + _view.Name);

            QueryVisitor qv = new QueryVisitor(QueryColumnExpression);
            qv.Visit(filter);
            var delbits = _deletedRows.GetBits();
            var ba = ((WAHBitArray)qv._bitmap.Pop()).AndNot(delbits);
            List<TSchema> trows = null;
            if (_view.TransactionMode)
            {
                // query from transaction own data
                tran_data data = null;
                if (_transactions.TryGetValue(Thread.CurrentThread.ManagedThreadId, out data))
                {
                    var rrows = new List<TSchema>();
                    foreach (var kv in data.rows)
                    {
                        foreach (var r in kv.Value)
                        {
                            rrows.Add(_rowfill(r));
                        }
                    }
                    trows = rrows.FindAll(filter.Compile());
                }
            }

            var order = SortBy(orderby);
            bool desc = orderby != null && orderby.EndsWith(" desc", StringComparison.InvariantCultureIgnoreCase);
            _log.Debug("query bitmap done (ms) : " + FastDateTime.Now.Subtract(dt).TotalMilliseconds);
            // exec query return rows
            return ReturnRows(ba, trows, start, count, order, desc);
        }

        IResult IViewHandler.Query(int start, int count)
        {
            return Query(start, count);
        }
        public Result<TSchema> Query(int start, int count)
        {
            return Query(start, count, null);
        }

        IResult IViewHandler.Query(int start, int count, string orderby)
        {
            return Query(start, count, orderby);
        }
        public Result<TSchema> Query(int start, int count, string orderby)
        {
            // no filter query -> just show all the data
            DateTime dt = FastDateTime.Now;
            _log.Debug("query : " + _view.Name);
            int totalviewrows = _viewData.Count();
            var rows = new List<TSchema>();
            var ret = new Result<TSchema>();
            int skip = start;
            int cc = 0;
            WAHBitArray del = _deletedRows.GetBits();
            ret.TotalCount = totalviewrows - (int)del.CountOnes();

            var order = SortBy(orderby);
            bool desc = false;
            if (orderby.ToLower().Contains(" desc"))
                desc = true;
            if (order.Count == 0)
                for (int i = 0; i < totalviewrows; i++)
                    order.Add(i);

            if (count == -1)
                count = totalviewrows;
            int len = order.Count;
            if (desc == false)
            {
                for (int idx = 0; idx < len; idx++)
                {
                    OutputRow(rows, idx, count, ref skip, ref cc, del, order);
                    if (cc == count) break;
                }
            }
            else
            {
                for (int idx = len - 1; idx >= 0; idx--)
                {
                    OutputRow(rows, idx, count, ref skip, ref cc, del, order);
                    if (cc == count) break;
                }
            }

            _log.Debug("query rows fetched (ms) : " + FastDateTime.Now.Subtract(dt).TotalMilliseconds);
            _log.Debug("query rows count : " + rows.Count.ToString("#,0"));
            ret.OK = true;
            ret.Count = rows.Count;
            //ret.TotalCount = rows.Count;
            ret.Rows = rows;
            return ret;
        }


        /// <summary>
        /// Adds item with idnex 'idx' to 'rows'
        /// </summary>
        /// <returns>if something was added</returns>
        private bool OutputRow(List<TSchema> rows, int idx)
        {
            byte[] b = _viewData.ViewReadRawBytes(idx);
            if (b != null)
            {
                object[] data = (object[])fastBinaryJSON.BJSON.ToObject(b);
                rows.Add(_rowfill(data));
                return true;
            }
            return false;
        }

        /// <summary>
        /// Adds item with idnex 'order[idx]' to 'rows' if skipped == 0 and not in del 
        /// </summary>
        private void OutputRow(List<TSchema> rows, int idx, int count, ref int skip, ref int currentCount, WAHBitArray del, List<int> order)
        {
            int i = order[idx];
            if (del.Get(i) == false)
            {
                if (skip > 0)
                    skip--;
                else
                {
                    bool b = OutputRow(rows, i);
                    if (b && count > 0)
                        currentCount++;
                }
            }
        }

        private void extractsortrowobject(WAHBitArray ba, int count, List<int> orderby, List<TSchema> rows, ref int skip, ref int c, int idx)
        {
            int i = orderby[idx];
            if (ba.Get(i))
            {
                if (skip > 0)
                    skip--;
                else
                {
                    bool b = OutputRow(rows, i);
                    if (b && count > 0)
                        c++;
                }
                ba.Set(i, false);
            }
        }

        public void Shutdown()
        {
            try
            {
                _saveTimer.Enabled = false;
                while (_stsaving)
                    Thread.Sleep(1);

                if (_rebuilding)
                    _log.Debug("Waiting for view rebuild to finish... : " + _view.Name);

                while (_rebuilding)
                    Thread.Sleep(50);

                _log.Debug("Shutting down Viewhandler");
                // shutdown indexes
                foreach (var v in _indexes)
                {
                    _log.Debug("Shutting down view index : " + v.Key);
                    v.Value.Shutdown();
                }
                // save deletedbitmap
                _deletedRows.Shutdown();

                _viewData.Shutdown();

                // write view version
                File.WriteAllText(_path + _view.Name + ".version", _view.Version.ToString());

                File.WriteAllText(_path + _RaptorDBVersionFilename, _RaptorDBVersion.ToString());
                // remove dirty file
                if (File.Exists(_path + _dirtyFilename))
                    File.Delete(_path + _dirtyFilename);
                _log.Debug("Viewhandler shutdown done.");
            }
            catch (Exception ex)
            {
                _log.Error(ex);
            }
        }

        public void Delete(Guid docid)
        {
            DeleteRowsWith(docid);

            InvalidateSortCache();
        }

        #region [  private methods  ]

        private Result<TSchema> ReturnRows(WAHBitArray ba, List<TSchema> trows, int start, int count, List<int> orderby, bool descending)
        {
            DateTime dt = FastDateTime.Now;
            List<TSchema> rows = new List<TSchema>();
            Result<TSchema> ret = new Result<TSchema>();
            int skip = start;
            int c = 0;
            ret.TotalCount = (int)ba.CountOnes();
            if (count == -1) count = ret.TotalCount;
            if (count > 0)
            {
                if (orderby != null && orderby.Count > 0)
                {
                    int len = orderby.Count;
                    if (descending == false)
                    {
                        for (int idx = 0; idx < len; idx++)
                        {
                            extractsortrowobject(ba, count, orderby, rows, ref skip, ref c, idx);
                            if (c == count) break;
                        }
                    }
                    else
                    {
                        for (int idx = len - 1; idx >= 0; idx--)
                        {
                            extractsortrowobject(ba, count, orderby, rows, ref skip, ref c, idx);
                            if (c == count) break;
                        }
                    }
                }
                foreach (int i in ba.GetBitIndexes())
                {
                    if (c < count)
                    {
                        if (skip > 0)
                            skip--;
                        else
                        {
                            bool b = OutputRow(rows, i);
                            if (b && count > 0)
                                c++;
                        }
                        if (c == count) break;
                    }
                }
            }
            if (trows != null) // TODO : move to start and decrement in count
                foreach (var o in trows)
                    rows.Add(o);
            _log.Debug("query rows fetched (ms) : " + FastDateTime.Now.Subtract(dt).TotalMilliseconds);
            _log.Debug("query rows count : " + rows.Count.ToString("#,0"));
            ret.OK = true;
            ret.Count = rows.Count;
            ret.Rows = rows;
            return ret;
        }

        MethodInfo insertmethod = null;
        bool _rebuilding = false;
        private void RebuildFromScratch(IDocStorage<Guid> docs)
        {
            _rebuilding = true;
            try
            {
                _log.Debug("Rebuilding view from scratch...");
                _log.Debug("View = " + _view.Name);
                DateTime dt = FastDateTime.Now;

                int c = docs.RecordCount();
                for (int i = 0; i < c; i++)
                {
                    StorageItem<Guid> meta = null;
                    object obj = docs.GetObject(i, out meta);
                    if (meta != null && meta.isDeleted)
                        Delete(meta.key);
                    else if (obj is View_delete)
                    {
                        View_delete vd = (View_delete)obj;
                        if (vd.Viewname.Equals(this._view.Name, StringComparison.InvariantCultureIgnoreCase))
                            ViewDelete(vd.Filter);
                    }
                    else if (obj is View_insert)
                    {
                        View_insert vi = (View_insert)obj;
                        if (vi.Viewname.Equals(this._view.Name.ToLower(), StringComparison.InvariantCultureIgnoreCase))
                            ViewInsert(vi.ID, vi.RowObject);
                    }
                    else if (obj is TDoc)
                    {
                        Insert(meta.key, (TDoc)obj);
                    }
                }
                _log.Debug("rebuild view '" + _view.Name + "' done (s) = " + FastDateTime.Now.Subtract(dt).TotalSeconds);

                // write version.dat file when done
                File.WriteAllText(_path + _view.Name + ".version", _view.Version.ToString());
            }
            catch (Exception ex)
            {
                _log.Error("Rebuilding View failed : " + _view.Name, ex);
            }
            _rebuilding = false;
        }

        private void CreateLoadIndexes(ViewRowDefinition viewRowDefinition)
        {
            _indexes.Add(_docid, new TypeIndexes<Guid>(_path, _docid, 16/*, allowDups: !_view.DeleteBeforeInsert*/));
            // load indexes
            foreach (var c in viewRowDefinition.Columns)
            {
                if (c.Key != "docid")
                    _indexes.Add(c.Key, c.Value.CreateIndex(_path, c.Key));
            }
        }

        private void InsertRowsWithIndexUpdate(Guid guid, List<object[]> rows)
        {
            if (_isDirty == false)
                WriteDirtyFile();

            foreach (var row in rows)
            {
                object[] r = new object[row.Length + 1];
                r[0] = guid;
                Array.Copy(row, 0, r, 1, row.Length);
                byte[] b = fastBinaryJSON.BJSON.ToBJSON(r);

                int rownum = (int)_viewData.WriteRawData(b);

                IndexRow(guid, row, rownum);
            }
        }

        private void IndexRow(Guid id, object[] row, int rownum)
        {
            _indexes[_docid].Set(id, rownum);
            for (int i = 0; i < row.Length; i++)
            {
                _indexes[_colnames[i]].Set(row[i], rownum);
            }
        }


        private void DeleteRowsWith(Guid guid)
        {
            // find bitmap for guid column
            WAHBitArray gc = QueryColumnExpression(_docid, RDBExpression.Equal, guid);
            _deletedRows.InPlaceOR(gc);
        }

        private WAHBitArray QueryColumnExpression(string colname, RDBExpression exp, object from)
        {
            return _indexes[colname].Query(exp, from, _viewData.Count());
        }

        SafeDictionary<string, Expression<Predicate<TSchema>>> _lambdacache = new SafeDictionary<string, Expression<Predicate<TSchema>>>();
        private Expression<Predicate<TSchema>> ParseFilter(string filter)
        {
            if (filter == null) return null;
            filter = filter.Trim();
            if (filter.Length == 0) return null;
            Expression<Predicate<TSchema>> le = null;
            if (_lambdacache.TryGetValue(filter, out le) == false)
            {
                le = System.Linq.Dynamic.DynamicExpression.ParseLambda<Predicate<TSchema>>(_view.Schema, typeof(bool), filter, null);
                _lambdacache.Add(filter, le);
            }
            return le;
        }

        #endregion

        #region Count
        public int Count(Expression<Predicate<TSchema>> filter)
        {
            int totcount = 0;
            DateTime dt = FastDateTime.Now;
            if (filter == null)
                totcount = TotalCount();
            else
            {
                WAHBitArray ba = new WAHBitArray();

                QueryVisitor qv = new QueryVisitor(QueryColumnExpression);
                qv.Visit(filter.Body);
                var delbits = _deletedRows.GetBits();
                ba = ((WAHBitArray)qv._bitmap.Pop()).AndNot(delbits);

                totcount = (int)ba.CountOnes();
            }
            _log.Debug("Count items = " + totcount);
            _log.Debug("Count time (ms) : " + FastDateTime.Now.Subtract(dt).TotalMilliseconds);
            return totcount;
        }

        public int Count(string filter)
        {
            return Count(ParseFilter(filter));
        }

        public int TotalCount()
        {
            int c = _viewData.Count();
            int cc = (int)_deletedRows.GetBits().CountOnes();
            return c - cc;
        }
        #endregion

        private SafeDictionary<string, List<int>> _sortcache = new SafeDictionary<string, List<int>>();

        private List<int> SortBy(string sortcol)
        {
            if (string.IsNullOrEmpty(sortcol))
                return null;

            string col = _colnames.FirstOrDefault(c => sortcol.StartsWith(sortcol, StringComparison.InvariantCultureIgnoreCase));
            if (col == null)
            {
                _log.Debug("sort column not recognized : " + sortcol);
                return null;
            }

            DateTime dt = FastDateTime.Now;

            List<int> sortlist;
            if (!_sortcache.TryGetValue(col, out sortlist))
            {
                sortlist = new List<int>();
                int count = _viewData.Count();
                IIndex idx = _indexes[col];
                object[] keys = idx.GetKeys();
                Array.Sort(keys);

                foreach (var k in keys)
                {
                    var bi = idx.Query(RDBExpression.Equal, k, count).GetBitIndexes();
                    foreach (var i in bi)
                        sortlist.Add(i);
                }
                _sortcache.Add(col, sortlist);
            }
            _log.Debug("Sort column = " + col + ", time (ms) = " + FastDateTime.Now.Subtract(dt).TotalMilliseconds);
            return sortlist;
        }

        public ViewRowDefinition GetSchema()
        {
            return _schema;
        }

        int _lastrownumber = -1;
        object _rowlock = new object();
        public int NextRowNumber()
        {
            // TODO: interlocked
            lock (_rowlock)
            {
                if (_lastrownumber == -1)
                    _lastrownumber = TotalCount();
                return ++_lastrownumber;
            }
        }

        /// <summary>
        /// marks matching items as removed
        /// </summary>
        /// <returns>Count of removed items</returns>
        public int ViewDelete(Expression<Predicate<TSchema>> filter)
        {
            if (filter == null) return 0;
            _log.Debug("delete : " + _view.Name);
            if (_isDirty == false)
                WriteDirtyFile();
            QueryVisitor qv = new QueryVisitor(QueryColumnExpression);
            qv.Visit(filter.Body);
            var delbits = _deletedRows.GetBits();
            int count = qv._bitmap.Count;
            if (count > 0)
            {
                WAHBitArray qbits = (WAHBitArray)qv._bitmap.Pop();
                _deletedRows.InPlaceOR(qbits);
                count = (int)qbits.CountOnes();
            }
            _log.Debug("Deleted rows = " + count);

            InvalidateSortCache();
            return count;
        }
        /// <summary>
        /// marks matching items as removed
        /// </summary>
        /// <returns>Count of removed items</returns>
        public int ViewDelete(string filter)
        {
            return ViewDelete(ParseFilter(filter));
        }

        private object _dfile = new object();
        private void WriteDirtyFile()
        {
            lock (_dfile)
            {
                _isDirty = true;
                if (File.Exists(_path + _dirtyFilename) == false)
                    File.WriteAllText(_path + _dirtyFilename, "dirty");
            }
        }


        public bool ViewInsert(Guid id, object row)
        {
            List<object> l = new List<object>();
            l.Add(row);

            var r = ViewSchemaHelper.ExtractRows(l, _colnames);
            InsertRowsWithIndexUpdate(id, r);

            InvalidateSortCache();
            return true;
        }

        private void InvalidateSortCache()
        {
            _sortcache = new SafeDictionary<string, List<int>>();
        }
    }
}
