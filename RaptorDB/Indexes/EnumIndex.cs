using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace RaptorDB.Indexes
{
    internal class EnumIntIndex<T> : MGIndex<int>, IIndex where T : struct, IConvertible
    {
        public EnumIntIndex(string path, string filename)
            : base(path, filename + ".mgidx", 4, Global.PageItemCount, true)
        {
        }

        public void Set(object key, int recnum)
        {
            if (key == null) return;
            base.Set((int)key, recnum);
        }

        public WAHBitArray Query(RDBExpression ex, object from, int maxsize)
        {
            if (!typeof(T).Equals(from.GetType()))
                from = Converter(from);

            return base.Query(ex, (int)from, maxsize);
        }

        private T Converter(object from)
        {
            if (typeof(T) == typeof(Guid))
            {
                object o = new Guid(from.ToString());
                return (T)o;
            }
            else
                return (T)Convert.ChangeType(from, typeof(T));
        }

        void IIndex.FreeMemory()
        {
            base.FreeMemory();
            base.SaveIndex();
        }

        void IIndex.Shutdown()
        {
            base.SaveIndex();
            base.Shutdown();
        }

        public WAHBitArray Query(object fromkey, object tokey, int maxsize)
        {
            if (typeof(T).Equals(fromkey.GetType()) == false)
                fromkey = Convert.ChangeType(fromkey, typeof(T));

            if (typeof(T).Equals(tokey.GetType()) == false)
                tokey = Convert.ChangeType(tokey, typeof(T));

            return base.Query((int)fromkey, (int)tokey, maxsize);
        }

        object[] IIndex.GetKeys()
        {
            return base.GetKeys();
        }
    }
}
