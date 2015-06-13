using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using RaptorDB.Common;
using System.Reflection;
using System.IO;
using System.Threading.Tasks;
using System.Threading;
using System.Linq.Expressions;

namespace RaptorDB
{
    public delegate void Handler(Packet data, ReturnPacket ret);

    public class RaptorDBServer
    {
        public RaptorDBServer(int port, string DataPath)
        {
            _path = Directory.GetCurrentDirectory();
            AppDomain.CurrentDomain.AssemblyResolve += new ResolveEventHandler(CurrentDomain_AssemblyResolve);
            _server = new NetworkServer();

            if (_S == "/")// unix system
                _datapath = DataPath.Replace("\\", "/");
            else
                _datapath = DataPath;

            if (_datapath.EndsWith(_S) == false)
                _datapath += _S;

            _raptor = RaptorDB.Open(DataPath);
            register = _raptor.GetType().GetMethod("RegisterView", BindingFlags.Instance | BindingFlags.Public);
            save = _raptor.GetType().GetMethod("Save", BindingFlags.Instance | BindingFlags.Public);
            Initialize();
            _server.Start(port, processpayload);
        }

        private string _S = Path.DirectorySeparatorChar.ToString();
        private Dictionary<string, uint> _users = new Dictionary<string, uint>();
        private string _path = "";
        private string _datapath = "";
        private ILog _log = LogManager.GetLogger(typeof(RaptorDBServer));
        private NetworkServer _server;
        private RaptorDB _raptor;
        private MethodInfo register = null;
        private MethodInfo save = null;
        private SafeDictionary<Type, MethodInfo> _savecache = new SafeDictionary<Type, MethodInfo>();
        private SafeDictionary<string, ServerSideFuncInfo> _ssidecache = new SafeDictionary<string, ServerSideFuncInfo>();
        private Dictionary<PacketCommand, Handler> _handlers = new Dictionary<PacketCommand, Handler>();
        private const string _RaptorDB_users_config = "RaptorDB-Users.config";

        private Assembly CurrentDomain_AssemblyResolve(object sender, ResolveEventArgs args)
        {
            if (File.Exists(args.Name))
                return Assembly.LoadFrom(args.Name);
            string[] ss = args.Name.Split(',');
            string fname = ss[0] + ".dll";
            if (File.Exists(fname))
                return Assembly.LoadFrom(fname);
            fname = "Extensions" + _S + fname;
            if (File.Exists(fname))
                return Assembly.LoadFrom(fname);
            else return null;
        }

        private MethodInfo GetSave(Type type)
        {
            MethodInfo m = null;
            if (_savecache.TryGetValue(type, out m))
                return m;

            m = save.MakeGenericMethod(new Type[] { type });
            _savecache.Add(type, m);
            return m;
        }

        public void Shutdown()
        {
            WriteUsers();
            _server.Stop();
            _raptor.Shutdown();
        }

        private void WriteUsers()
        {
            // write users to user.config file
            StringBuilder sb = new StringBuilder();
            sb.AppendLine("# FORMAT : username , pasword hash");
            sb.AppendLine("# To disable a user comment the line with the '#'");
            foreach (var kv in _users)
            {
                sb.AppendLine(kv.Key + " , " + kv.Value);
            }

            File.WriteAllText(_datapath + _RaptorDB_users_config, sb.ToString());
        }

        private object processpayload(object data)
        {
            Packet p = (Packet)data;

            if (Authenticate(p) == false)
                return new ReturnPacket(false, "Authentication failed");

            ReturnPacket ret = new ReturnPacket(true);
            try
            {
                Handler d = null;
                ret.OK = true;
                if (_handlers.TryGetValue(p.Command, out d))
                    d(p, ret);
                else
                    _log.Error("Command handler not found : " + p.Command);
            }
            catch (Exception ex)
            {
                ret.OK = false;
                ret.Error = ex.GetType().Name + ": " + ex.Message;
                _log.Error(ex);
            }
            return ret;
        }

        private void InitializeCommandsDictionary()
        {
            _handlers.Add(PacketCommand.Save,
                (p, ret) =>
                {
                    var m = GetSave(p.Data.GetType());
                    m.Invoke(_raptor, new object[] { p.Docid, p.Data });
                });

            _handlers.Add(PacketCommand.SaveBytes,
                (p, ret) =>
                {
                    ret.OK = _raptor.SaveBytes(p.Docid, (byte[])p.Data);
                });

            _handlers.Add(PacketCommand.QueryType,
                (p, ret) =>
                {
                    var param = (object[])p.Data;
                    Type t = Type.GetType((string)param[0]);
                    string viewname = _raptor.GetViewName(t);
                    ret.Data = _raptor.Query(viewname, (string)param[1], p.Start, p.Count, p.OrderBy);
                });

            _handlers.Add(PacketCommand.QueryStr,
                (p, ret) =>
                {
                    ret.Data = _raptor.Query(p.Viewname, (string)p.Data, p.Start, p.Count, p.OrderBy);
                });

            _handlers.Add(PacketCommand.Fetch,
                (p, ret) =>
                {
                    ret.Data = _raptor.Fetch(p.Docid);
                });

            _handlers.Add(PacketCommand.FetchBytes,
                (p, ret) =>
                {
                    ret.OK = true;
                    ret.Data = _raptor.FetchBytes(p.Docid);
                });

            _handlers.Add(PacketCommand.Backup,
                (p, ret) =>
                {
                    ret.OK = _raptor.Backup();
                });

            _handlers.Add(PacketCommand.Delete,
                (p, ret) =>
                {
                    ret.OK = _raptor.Delete(p.Docid);
                });

            _handlers.Add(PacketCommand.DeleteBytes,
                (p, ret) =>
                {
                    ret.OK = _raptor.DeleteBytes(p.Docid);
                });

            _handlers.Add(PacketCommand.Restore,
                (p, ret) =>
                {
                    Task.Factory.StartNew(() => _raptor.Restore());
                });

            _handlers.Add(PacketCommand.AddUser,
                (p, ret) =>
                {
                    var param = (object[])p.Data;
                    ret.OK = AddUser((string)param[0], (string)param[1], (string)param[2]);
                });

            _handlers.Add(PacketCommand.ServerSide,
                (p, ret) =>
                {
                    var param = (object[])p.Data;
                    ret.Data = _raptor.ServerSide(GetServerSideFuncCache(param[0].ToString(), param[1].ToString()).GetFunc(param[2]), (string)param[3]);
                });

            _handlers.Add(PacketCommand.FullText,
                (p, ret) =>
                {
                    var param = (object[])p.Data;
                    ret.Data = _raptor.FullTextSearch((string)param[0]);
                });

            _handlers.Add(PacketCommand.CountType,
                (p, ret) =>
                {
                    // count type
                    var param = (object[])p.Data;
                    Type t = Type.GetType((string)param[0]);
                    string viewname = _raptor.GetViewName(t);
                    ret.Data = _raptor.Count(viewname, (string)param[1]);
                });

            _handlers.Add(PacketCommand.CountStr,
                (p, ret) =>
                {
                    // count str
                    ret.Data = _raptor.Count(p.Viewname, (string)p.Data);
                });

            _handlers.Add(PacketCommand.GCount,
                (p, ret) =>
                {
                    Type t = Type.GetType(p.Viewname);
                    string viewname = _raptor.GetViewName(t);
                    ret.Data = _raptor.Count(viewname, (string)p.Data);
                });

            _handlers.Add(PacketCommand.DocHistory,
                (p, ret) =>
                {
                    ret.Data = _raptor.FetchHistory(p.Docid);
                });

            _handlers.Add(PacketCommand.FileHistory,
                (p, ret) =>
                {
                    ret.Data = _raptor.FetchBytesHistory(p.Docid);
                });

            _handlers.Add(PacketCommand.FetchVersion,
                (p, ret) =>
                {
                    ret.Data = _raptor.FetchVersion((int)p.Data);
                });

            _handlers.Add(PacketCommand.FetchFileVersion,
                (p, ret) =>
                {
                    ret.Data = _raptor.FetchBytesVersion((int)p.Data);
                });

            _handlers.Add(PacketCommand.CheckAssembly,
                (p, ret) =>
                {
                    string typ = "";
                    ret.Data = _raptor.GetAssemblyForView(p.Viewname, out typ);
                    ret.Error = typ;
                });
            _handlers.Add(PacketCommand.FetchHistoryInfo,
                (p, ret) =>
                {
                    ret.Data = _raptor.FetchHistoryInfo(p.Docid);
                });

            _handlers.Add(PacketCommand.FetchByteHistoryInfo,
                (p, ret) =>
                {
                    ret.Data = _raptor.FetchBytesHistoryInfo(p.Docid);
                });

            _handlers.Add(PacketCommand.ViewDelete,
                (p, ret) =>
                {
                    var param = (object[])p.Data;
                    ret.Data = _raptor.ViewDelete((string)param[0], (string)param[1]);
                });

            _handlers.Add(PacketCommand.ViewDelete_t,
                (p, ret) =>
                {
                    var param = (object[])p.Data;
                    Type t = Type.GetType((string)param[0]);
                    string viewname = _raptor.GetViewName(t);
                    ret.Data = _raptor.ViewDelete(viewname, (string)param[1]);
                });

            _handlers.Add(PacketCommand.ViewInsert,
                (p, ret) =>
                {
                    var param = (object[])p.Data;
                    ret.Data = _raptor.ViewInsert((string)param[0], p.Docid, param[1]);
                });

            _handlers.Add(PacketCommand.ViewInsert_t,
                (p, ret) =>
                {
                    var param = (object[])p.Data;
                    Type t = Type.GetType((string)param[0]);
                    string viewname = _raptor.GetViewName(t);
                    ret.Data = _raptor.ViewInsert(viewname, p.Docid, param[1]);
                });

            _handlers.Add(PacketCommand.DocCount,
                (p, ret) =>
                {
                    ret.Data = _raptor.DocumentCount();
                });

            _handlers.Add(PacketCommand.GetObjectHF,
                (p, ret) =>
                {
                    ret.Data = _raptor.GetKVHF().GetObjectHF((string)p.Data);
                });

            _handlers.Add(PacketCommand.SetObjectHF,
                (p, ret) =>
                {
                    var param = (object[])p.Data;
                    _raptor.GetKVHF().SetObjectHF((string)param[0], param[1]);
                });

            _handlers.Add(PacketCommand.DeleteKeyHF,
                (p, ret) =>
                {
                    ret.Data = _raptor.GetKVHF().DeleteKeyHF((string)p.Data);
                });

            _handlers.Add(PacketCommand.CountHF,
                (p, ret) =>
                {
                    ret.Data = _raptor.GetKVHF().CountHF();
                });

            _handlers.Add(PacketCommand.ContainsHF,
                (p, ret) =>
                {
                    ret.Data = _raptor.GetKVHF().ContainsHF((string)p.Data);
                });

            _handlers.Add(PacketCommand.GetKeysHF,
                (p, ret) =>
                {
                    ret.Data = _raptor.GetKVHF().GetKeysHF();
                });

            _handlers.Add(PacketCommand.CompactStorageHF,
                (p, ret) =>
                {
                    _raptor.GetKVHF().CompactStorageHF();
                });
        }

        public delegate List<object> ServerSideFuncAnonymous(object target, IRaptorDB rap, string filter);
        public class ServerSideFuncInfo
        {
            public ServerSideFunc StaticFunc { get; set; }
            public ServerSideFuncAnonymous InstanceFunc { get; set; }

            public ServerSideFunc GetFunc(object target = null)
            {
                return StaticFunc ?? ((rap, filter) => InstanceFunc(target, rap, filter));
            }
        }
        private ServerSideFuncInfo GetServerSideFuncCache(string type, string method)
        {
            ServerSideFuncInfo func;
            _log.Debug("Calling Server side Function : " + method + " on type " + type);
            if (_ssidecache.TryGetValue(type + method, out func) == false)
            {
                func = new ServerSideFuncInfo();
                Type tt = Type.GetType(type);
                var methodInfo = Type.GetType(type).GetMethod(method, BindingFlags.Instance | BindingFlags.Static | BindingFlags.NonPublic | BindingFlags.Public);
                if (methodInfo == null) throw new ArgumentException("specified method not found on type");
                if (!methodInfo.IsStatic)
                {
                    var targetParEx = Expression.Parameter(typeof(object), "target");
                    var rapParEx = Expression.Parameter(typeof(IRaptorDB), "rap");
                    var filterParEx = Expression.Parameter(typeof(string), "filter");
                    var callEx = Expression.Call(Expression.Convert(targetParEx, methodInfo.DeclaringType), methodInfo, rapParEx, filterParEx);
                    func.InstanceFunc = Expression.Lambda<ServerSideFuncAnonymous>(callEx, targetParEx, rapParEx, filterParEx).Compile();
                }
                else
                {
                    var coreF = func.StaticFunc = (ServerSideFunc)Delegate.CreateDelegate(typeof(ServerSideFunc), methodInfo);
                    func.InstanceFunc = (t, r, f) => coreF(r, f);
                }
                _ssidecache.Add(type + method, func);
            }
            return func;
        }

        private uint GenHash(string user, string pwd)
        {
            return Helper.MurMur.Hash(Encoding.UTF8.GetBytes(user.ToLower() + "|" + pwd));
        }

        private bool AddUser(string user, string oldpwd, string newpwd)
        {
            uint hash = 0;
            if (_users.TryGetValue(user.ToLower(), out hash) == false)
            {
                _users.Add(user.ToLower(), GenHash(user, newpwd));
                return true;
            }
            if (hash == GenHash(user, oldpwd))
            {
                _users[user.ToLower()] = GenHash(user, newpwd);
                return true;
            }
            return false;
        }

        private bool Authenticate(Packet p)
        {
            uint pwd;
            if (_users.TryGetValue(p.Username.ToLower(), out pwd))
            {
                uint hash = uint.Parse(p.PasswordHash);
                if (hash == pwd) return true;
            }
            _log.Debug("Authentication failed for '" + p.Username + "' hash = " + p.PasswordHash);
            return false;
        }

        private void Initialize()
        {
            // load users here
            if (File.Exists(_datapath + _RaptorDB_users_config))
            {
                foreach (string line in File.ReadAllLines(_datapath + _RaptorDB_users_config))
                {
                    if (line.Contains("#") == false)
                    {
                        string[] s = line.Split(',');
                        _users.Add(s[0].Trim().ToLower(), uint.Parse(s[1].Trim()));
                    }
                }
            }
            // add default admin user if not exists
            if (_users.ContainsKey("admin") == false)
                _users.Add("admin", GenHash("admin", "admin"));

            // exe folder
            // |-Extensions
            Directory.CreateDirectory(_path + _S + "Extensions");

            // open extensions folder
            string path = _path + _S + "Extensions";

            foreach (var f in Directory.GetFiles(path, "*.dll"))
            {
                //        - load all dll files
                //        - register views 
                _log.Debug("loading dll for views : " + f);
                Assembly a = Assembly.Load(f);
                foreach (var t in a.GetTypes())
                {
                    foreach (var att in t.GetCustomAttributes(typeof(RegisterViewAttribute), false))
                    {
                        try
                        {
                            object o = Activator.CreateInstance(t);
                            //  handle types when view<T> also
                            Type[] args = t.GetGenericArguments();
                            if (args.Length == 0)
                                args = t.BaseType.GetGenericArguments();
                            Type tt = args[0];
                            var m = register.MakeGenericMethod(new Type[] { tt });
                            m.Invoke(_raptor, new object[] { o });
                        }
                        catch (Exception ex)
                        {
                            _log.Error(ex);
                        }
                    }
                }
            }

            InitializeCommandsDictionary();
        }
    }
}
