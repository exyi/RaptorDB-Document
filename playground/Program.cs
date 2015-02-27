using RaptorDB.Common;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Faker;
using System.Diagnostics;
using System.Threading;
using System.Collections.Concurrent;

namespace playground
{
    static class Program
    {
        static void Main(string[] args)
        {
            new RRRandom().HackToFaker();

            Console.WriteLine("opening db");
            var rap = OpenDB();
            if (rap.Count("ModelItem") == 0) Insert(rap, 200000);
            // UpdateHF(rap, 3);
            QueryTest(rap);
            // rap.Shutdown();
        }

        static void QueryTest(IRaptorDB rap)
        {
            while (true)
            {
                Console.Write("> ");
                var query = Console.ReadLine().Split(':');
                if (query.Length == 0) continue;
                var sw = new Stopwatch();
                if (query[0] == "q" || query[0] == "c")
                {
                    string view = "ModelItem";
                    string filter = "";
                    int skip = 0;
                    int limit = 10;
                    if (query.Length > 2)
                    {
                        view = query[1];
                        filter = query[2];
                    }
                    else if (query.Length == 2)
                    {
                        filter = query[1];
                    }
                    if (query.Length >= 5)
                    {
                        if (!int.TryParse(query[3], out skip))
                            skip = 0;
                        if (!int.TryParse(query[4], out limit))
                            limit = 10;
                    }
                    if (query[0] == "q")
                    {
                        sw.Start();
                        var result = rap.Query(view, filter, skip, limit);
                        sw.Stop();
                        if (result.OK)
                        {
                            foreach (var r in result.Rows)
                            {
                                Print(r);
                            }
                        }
                    }
                    else if (query[0] == "c")
                    {
                        sw.Start();
                        var c = rap.Count(view, filter);
                        sw.Stop();
                        Console.WriteLine(c);
                    }
                }
                else if (query[0] == "hf")
                {
                    if (query.Length == 2)
                    {
                        sw.Start();
                        var doc = rap.GetKVHF().GetObjectHF(query[1]);
                        sw.Stop();
                        Print(doc);
                    }
                    else if (query.Length == 1)
                    {
                        sw.Start();
                        var doc = rap.GetKVHF().GetKeysHF();
                        sw.Stop();
                        Print(doc);
                    }
                }
                else if (query[0] == "f")
                {
                    var id = Guid.Parse(query[1]);
                    sw.Start();
                    var doc = rap.Fetch(id);
                    sw.Stop();
                    Print(doc);
                }
                else if (query[0] == "exit") return;
                Console.WriteLine("{0}", sw.Elapsed.ToString());
            }
        }

        static void Print(object o)
        {
            Console.WriteLine(fastJSON.JSON.ToNiceJSON(o, new fastJSON.JSONParameters
            {
                UseFastGuid = false,
                UsingGlobalTypes = false
            }));
        }

        static void Insert(IRaptorDB rap, int count)
        {
            int i = 0;
            Console.WriteLine("generating items");
            var items = GenerateItems(count);
            var sw = Stopwatch.StartNew();
            Console.WriteLine("inserting items");
            Parallel.ForEach(items, new ParallelOptions() { MaxDegreeOfParallelism = 8 }, item =>
            {
                Interlocked.Increment(ref i);
                rap.Save(item.Id, item);
                if (i % 500 == 0) Console.WriteLine("{0} items inserted in {1:N1}s", i, sw.ElapsedMilliseconds / 1000.0);
            });
        }

        static void UpdateHF(IRaptorDB rap, int count)
        {
            var hf = rap.GetKVHF();
            for (int i = 0; i < count; i++)
            {
                var name = NameFaker.Name();
                var obj = new OtherItem()
                {
                    Numbers = new bool[3].Select(n => NumberFaker.Number()).ToArray(),
                    Bytes = new Dictionary<string, byte[]>() {
                        {
                            "a",
                            fastBinaryJSON.BJSON.ToBJSON("{}")
                        }
                    }
                };
                hf.SetObjectHF(name, obj);
            }
        }

        public static RaptorDB.RaptorDB OpenDB()
        {
            RaptorDB.Global.EarlyPageSplitSize = 50;
            if (Directory.Exists("rdb")) Directory.Delete("rdb", recursive: true);
            var r = RaptorDB.RaptorDB.Open("rdb");
            r.RegisterView(new DefaultModelItemView());
            r.RegisterView(new FriendsModelItemView());
            return r;
        }

        public static IEnumerable<T> OneThreadBuffered<T>(this IEnumerable<T> source, int buffer = 1000)
        {
            int i = 0;
            var arr = new T[buffer];
            foreach (var el in source)
            {
                arr[i++] = el;
                if (i == buffer)
                {
                    foreach (var a in arr)
                        yield return a;
                    arr = new T[buffer];
                    i = 0;
                }
            }

            for (int j = 0; j < i; j++)
            {
                yield return arr[j];
            }
        }

        public static IEnumerable<ModelItem> GenerateItems(int count)
        {
            return new bool[count].Select(a =>
            {

                var i = new ModelItem();
                i.Id = Guid.NewGuid();
                i.Name = NameFaker.Name();
                i.Number = Faker.NumberFaker.Number(1, 500);
                i.Friends = new bool[NumberFaker.Number(1, 400)]
                    .Select(_ => NameFaker.Name()).ToArray();
                i.WebSite = Faker.InternetFaker.Domain();
                return i;
            });
        }
    }
}
