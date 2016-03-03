using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Linq.Expressions;
using System.Threading.Tasks;
using System.Threading;
using RaptorDB.Common;
using System.Reflection;

namespace RaptorDB.Views
{
    public class ViewManager
    {
        public ViewManager(string viewfolder, IDocStorage<Guid> objstore)
        {
            _Path = viewfolder;
            _objectStore = objstore;
        }

        private IDocStorage<Guid> _objectStore;
        private ILog _log = LogManager.GetLogger(typeof(ViewManager));
        private string _Path = "";
        // list of views
        private Dictionary<string, IViewHandler> _views = new Dictionary<string, IViewHandler>(StringComparer.InvariantCultureIgnoreCase);
        // primary view list
        private Dictionary<Type, string> _primaryView = new Dictionary<Type, string>();
        // like primary view list 
        private Dictionary<Type, string> _otherViewTypes = new Dictionary<Type, string>();
        // consistent views
        private Dictionary<Type, List<string>> _consistentViews = new Dictionary<Type, List<string>>();
        // other views type->list of view names to call
        private Dictionary<Type, List<string>> _otherViews = new Dictionary<Type, List<string>>();
        private TaskQueue _que = new TaskQueue();
        private SafeDictionary<int, bool> _transactions = new SafeDictionary<int, bool>();

        IViewHandler GetHandler(string name)
        {
            IViewHandler view = null;
            if (_views.TryGetValue(name, out view))
                return view;
            throw new ViewNotFoundException(name);
        }

        IViewHandler<TSchema> GetHandler<TSchema>(string name)
        {
            IViewHandler view = null;
            if (_views.TryGetValue(name, out view))
                return (IViewHandler<TSchema>)view;
            throw new ViewNotFoundException(name);
        }

        public int Count(string viewname, string filter)
        {
            return GetHandler(viewname).Count(filter);
        }

        public IResult Query(string viewname, string filter, int start, int count)
        {
            return Query(viewname, filter, start, count, null);
        }

        public IResult Query(string viewname, int start, int count)
        {
            return GetHandler(viewname).Query(start, count);
        }

        public void Insert<T>(string viewname, Guid docid, T data)
        {
            var handler = GetHandler(viewname);
            if (!handler.IsActive)
            {
                _log.Debug("view is not active, skipping insert : " + viewname);
            }
            else if (handler.BackgroundIndexing)
                _que.AddTask(() => handler.Insert(docid, data));
            else
                handler.Insert(docid, data);

            return;
        }

        public bool InsertTransaction<T>(string viewname, Guid docid, T data)
        {
            IViewHandler vman = GetHandler(viewname);
            if (!vman.IsActive)
            {
                _log.Debug("view is not active, skipping insert : " + viewname);
                return false;
            }

            return vman.InsertTransaction(docid, data);
        }

        public object Fetch(Guid guid)
        {
            object b = null;
            _objectStore.GetObject(guid, out b);

            return b;
        }

        public string GetPrimaryViewForType(Type type)
        {
            string vn;
            if (type == null || type == typeof(object)) // reached the end
                return null;
            // find direct
            if (_primaryView.TryGetValue(type, out vn))
                return vn;
            // recurse basetype
            return GetPrimaryViewForType(type.BaseType);
        }

        public List<string> GetOtherViewsList(Type type)
        {
            List<string> list = new List<string>();
            _otherViews.TryGetValue(type, out list);
            return list;
        }

        public string GetViewName(Type type) // used for queries
        {
            string viewname = GetPrimaryViewForType(type);
            if (viewname != null)
                return viewname;

            // search for viewtype here
            if (_otherViewTypes.TryGetValue(type, out viewname))
                return viewname;

            return null;
        }

        public void RegisterView<TDoc, TSchema>(View<TDoc, TSchema> view)
        {
            view.Verify();
            if (_views.ContainsKey(view.Name))
            {
                _log.Error("View already added and exists : " + view.Name);
            }
            else
            {
                var vh = new ViewHandler<TDoc, TSchema>(_Path, this);
                vh.SetView(view, _objectStore);
                _views.Add(view.Name, vh);
                _otherViewTypes.Add(view.GetType(), view.Name);

                // add view schema mapping 
                _otherViewTypes.Add(view.Schema, view.Name);

                Type basetype = vh.GetFireOnType();
                if (view.isPrimaryList)
                {
                    _primaryView.Add(basetype, view.Name);
                }
                else
                {
                    if (view.ConsistentSaveToThisView)
                        AddToViewList(_consistentViews, basetype, view.Name);
                    else
                        AddToViewList(_otherViews, basetype, view.Name);
                }
            }
        }
        public void RegisterView<TDoc>(View<TDoc> view)
        {
            view.Verify();
            if (_views.ContainsKey(view.Name))
            {
                _log.Error("View already added and exists : " + view.Name);
            }
            else
            {
                var type = typeof(ViewHandler<,>).MakeGenericType(typeof(TDoc), view.Schema);
                var vh = Activator.CreateInstance(type, _Path, this) as IViewHandler;
                vh.SetView(view, _objectStore);
                _views.Add(view.Name, vh);
                _otherViewTypes.Add(view.GetType(), view.Name);

                // add view schema mapping 
                _otherViewTypes.Add(view.Schema, view.Name);

                Type basetype = vh.GetFireOnType();
                if (view.isPrimaryList)
                {
                    _primaryView.Add(basetype, view.Name);
                }
                else
                {
                    if (view.ConsistentSaveToThisView)
                        AddToViewList(_consistentViews, basetype, view.Name);
                    else
                        AddToViewList(_otherViews, basetype, view.Name);
                }
            }
        }

        public void ShutDown()
        {
            _log.Debug("View Manager shutdown");
            // shutdown views
            foreach (var v in _views)
            {
                try
                {
                    _log.Debug(" shutting down view : " + v.Value.View.Name);
                    v.Value.Shutdown();
                }
                catch (Exception ex)
                {
                    _log.Error(ex);
                }
            }
            _que.Shutdown();
        }

        public List<string> GetConsistentViews(Type type)
        {
            List<string> list = new List<string>();
            _consistentViews.TryGetValue(type, out list);
            return list;
        }

        private static void AddToViewList(IDictionary<Type, List<string>> diclist, Type fireontype, string viewname)
        {
            List<string> list = null;
            Type t = fireontype;// Type.GetType(tn);
            if (diclist.TryGetValue(t, out list))
                list.Add(viewname);
            else
            {
                list = new List<string>();
                list.Add(viewname);
                diclist.Add(t, list);
            }
        }

        public void Delete(Guid docid)
        {
            // remove from all views
            foreach (var v in _views)
                v.Value.Delete(docid);
        }

        public void Rollback(int ID)
        {
            _log.Debug("ROLLBACK");
            // rollback all views with tran id
            foreach (var v in _views)
                v.Value.RollBack(ID);

            _transactions.Remove(ID);
        }

        public void Commit(int ID)
        {
            _log.Debug("COMMIT");
            // commit all data in vews with tran id
            foreach (var v in _views)
                v.Value.Commit(ID);

            _transactions.Remove(ID);
        }

        public bool isTransaction(string viewname)
        {
            return _views[viewname.ToLower()].View.TransactionMode;
        }

        public bool inTransaction()
        {
            bool b = false;
            return _transactions.TryGetValue(Thread.CurrentThread.ManagedThreadId, out b);
        }

        public void StartTransaction()
        {
            _transactions.Add(Thread.CurrentThread.ManagedThreadId, false);
        }

        public Result<T> Query<T>(Expression<Predicate<T>> filter, int start, int count)
        {
            return Query<T>(filter, start, count, null);
        }

        public Result<T> Query<T>(Expression<Predicate<T>> filter, int start, int count, string orderby)
        {
            string view = GetViewName(typeof(T));
            return GetHandler<T>(view).Query(filter, start, count, orderby);
        }

        public Result<T> Query<T>(string filter, int start, int count)
        {
            return Query<T>(filter, start, count, null);
        }

        public Result<T> Query<T>(string filter, int start, int count, string orderby)
        {
            string view = GetViewName(typeof(T));

            return GetHandler<T>(view).Query(filter, start, count, orderby);
        }

        public int Count<T>(Expression<Predicate<T>> filter)
        {
            string view = GetViewName(typeof(T));
            return GetHandler<T>(view).Count(filter);
        }

        public void FreeMemory()
        {
            foreach (var v in _views)
                v.Value.FreeMemory();
        }

        public object GetAssemblyForView(string viewname, out string typename)
        {
            var schema = GetHandler(viewname).View.Schema;
            typename = schema.AssemblyQualifiedName;
            return System.IO.File.ReadAllBytes(schema.Assembly.Location);
        }

        public List<ViewBase> GetViews()
        {
            return _views.Values.Select(v => v.View).ToList();
        }

        public ViewRowDefinition GetSchema(string view)
        {
            return GetHandler(view).GetSchema();
        }

        public IResult Query(string viewname, string filter, int start, int count, string orderby)
        {
            return GetHandler(viewname).Query(filter, start, count, orderby);
        }

        public int ViewDelete<T>(Expression<Predicate<T>> filter)
        {
            string view = GetViewName(typeof(T));

            return GetHandler<T>(view).ViewDelete(filter);
        }

        public int ViewDelete(string viewname, string filter)
        {
            return GetHandler(viewname).ViewDelete(filter);
        }

        public bool ViewInsert<T>(Guid id, T row)
        {
            string view = GetViewName(typeof(T));

            return GetHandler<T>(view).ViewInsert(id, row);
        }

        public bool ViewInsert(string viewname, Guid id, object row)
        {
            return GetHandler(viewname).ViewInsert(id, row);
        }
    }
}
