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
using GenericPointerHelpers;
using System.Collections;

namespace playground
{
    static class Program
    {
        static void Main(string[] args)
        {
            new RRRandom().HackToFaker();

            //TestGrowingArray();
            //TestGrowingArray();

            //TestMemcp(16);
            TestMultiHashtable();
            TestMultiHashtable();
            TestMultiHashtable();
            TestMultiHashtable();
            TestMultiHashtable();
            TestMultiHashtable();
            Console.ReadLine();
            return;

            Console.WriteLine("opening db");
            var rap = OpenDB();
            if (rap.Count("ModelItem") == 0) Insert(rap, 30000);
            // UpdateHF(rap, 3);

            QueryTest(rap);
            // rap.Shutdown();
        }

        static void TestGrowingArray()
        {
            var sw = Stopwatch.StartNew();
            var list = new List<int>(1024);
            for (int i = 0; i < 0x100000; i++)
            {
                list.Add(i);
            }
            for (int i = 0; i < list.Count; i++)
            {
                list[i] *= 2;
            }
            Console.WriteLine("list: {0}", sw.Elapsed);
            sw.Restart();
            var array = new int[0x100000];
            var len = array.Length;
            for (int i = 0; i < 0x100000; i++)
            {
                if (i == len) Array.Resize(ref array, array.Length * 2);
                array[i] = i;
            }
            for (int i = 0; i < array.Length; i++)
            {
                array[i] *= 2;
            }
            Console.WriteLine("array.resize: {0}", sw.Elapsed);
            sw.Restart();
            var arrList = new List<int[]>(512);
            len = 1024;
            var curArr = new int[len];
            int index = 0;
            for (int i = 0; i < 0x100000; i++)
            {
                if (curArr.Length == index)
                {
                    curArr = new int[len];
                    arrList.Add(curArr);
                    index = 0;
                }
                curArr[index++] = i;
            }
            for (int i1 = 0; i1 < arrList.Count; i1++)
            {
                curArr = arrList[i1];
                for (int i = 0; i < curArr.Length; i++)
                {
                    curArr[i] *= 2;
                }
            }
            Console.WriteLine("list<array>: {0}", sw.Elapsed);
        }

        static unsafe void TestHashtable()
        {
            var names = Enumerable.Repeat(0, 100000).Select(i => Guid.NewGuid()).Distinct().ToArray();
            var dictionary = new Dictionary<Guid, int>(190000);
            var pht = PageHashTableHelper.CreateStructStruct<Guid, int>(12503*16);
            Console.WriteLine("testing");
            var sw = Stopwatch.StartNew();
            foreach (var name in names)
            {
                dictionary.Add(name, 120);
            }
            Console.WriteLine("dictionary write: {0}", sw.Elapsed);
            sw.Restart();
            foreach (var name in names)
            {
                var i = dictionary[name];
            }
            Console.WriteLine("dictionary read: {0}", sw.Elapsed);
            sw.Restart();
            foreach (var name in names)
            {
                pht.Set(name, 120);
            }
            Console.WriteLine("PageHashTable write: {0}", sw.Elapsed);
            sw.Restart();
            foreach (var name in names)
            {
                pht.FirstOrDefault(name);
            }
            Console.WriteLine("PageHashTable read: {0}", sw.Elapsed);
            HashtableDiagnostic(pht.GetBlockUsageBitmap());
            pht.Dispose();
        }

        static unsafe void TestMultiHashtable()
        {
            var random = new Random();
            var keys = Enumerable.Repeat(0, 100000).Select(i => random.Next(0, 20000)).ToArray();
            var dictionary = new Dictionary<int, int>(131072);
            var pht = PageHashTableHelper.CreateStructStructMulti<int, int>(131072);
            Console.WriteLine("testing");
            var sw = Stopwatch.StartNew();
            for (int i = 0; i < keys.Length; i++)
            {
                dictionary[keys[i]] = i;
            }
            Console.WriteLine("dictionary write: {0}", sw.Elapsed);
            sw.Restart();
            foreach (var name in keys)
            {
                var i = dictionary[name];
            }
            Console.WriteLine("dictionary read: {0}", sw.Elapsed);
            sw.Restart();
            for (int i = 0; i < keys.Length; i++)
            {
                pht.Set(keys[i], i);
            }
            Console.WriteLine("PageHashTable write: {0}", sw.Elapsed);
            sw.Restart();
            foreach (var name in keys)
            {
                var i = pht.FirstOrDefault(name);
            }
            Console.WriteLine("PageHashTable read: {0}", sw.Elapsed);
            HashtableDiagnostic(pht.GetBlockUsageBitmap());
            pht.Dispose();
        }

        public static void HashtableDiagnostic(BitArray ba)
        {
            List<int> clusters = new List<int>();
            int count = 0;
            bool value = false;
            for (int i = 1; i < ba.Count; i++)
            {
                var cv = ba.Get(i);
                if (cv == value) count++;
                else
                {
                    clusters.Add(count);
                    value = cv;
                    count = 1;
                }
            }
            Console.WriteLine("max: {0}", clusters.Max());
            Console.WriteLine("avg: {0}", clusters.Average());
            File.WriteAllLines("hashtableDiagnostic.txt", clusters.Select(c => c.ToString()));
        }

        static unsafe void TestMemcp(uint size)
        {
            const int iter = 100000;
            var from = new byte[size];
            var to = new byte[size];
            fixed (byte* fromPtr = from)
            {
                fixed (byte* toPtr = to)
                {
                    Console.WriteLine("testing memcpy, {0} bytes", size);
                    var sw = Stopwatch.StartNew();
                    for (int i = 0; i < iter; i++)
                    {
                        GenericPointerHelper.CopyBytes(fromPtr, toPtr, size);
                    }
                    Console.WriteLine("unaligned: {0}", sw.Elapsed);
                    sw.Restart();
                    for (int i = 0; i < iter; i++)
                    {
                        GenericPointerHelper.CopyBytesAlligned(fromPtr, toPtr, size);
                    }
                    Console.WriteLine("alligned: {0}", sw.Elapsed);
                    sw.Restart();
                    for (int i = 0; i < iter; i++)
                    {
                        for (int j = 0; j < size; j++)
                        {
                            *(toPtr + j) = *(fromPtr + j);
                        }
                    }
                    Console.WriteLine("stupidcopy: {0}", sw.Elapsed);
                    sw.Restart();
                    for (int i = 0; i < iter; i++)
                    {
                        Buffer.BlockCopy(from, 0, to, 0, (int)size);
                    }
                    Console.WriteLine("Buffer.BlockCopy: {0}", sw.Elapsed);
                    sw.Restart();
                    for (int i = 0; i < iter; i++)
                    {
                        Array.Copy(from, to, (int)size);
                    }
                    Console.WriteLine("Array.Copy: {0}", sw.Elapsed);
                }
            }
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
            //foreach(var item in items)
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
