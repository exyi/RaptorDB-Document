using RaptorDB.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace RaptorDB.Indexes
{
    internal class EnumIntIndex<T> : MGIndex<int>, IEqualsQueryIndex<T> where T : struct, IConvertible
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
        void IIndex.FreeMemory()
        {
            base.FreeMemory();
            base.SaveIndex();
        }

        public override void Dispose()
        {
            base.SaveIndex();
            base.Dispose();
        }

        T[] IIndex<T>.GetKeys()
        {
            throw new NotImplementedException("enum is not sortable");
        }

        public TResult Accept<TResult>(IIndexAcceptable<TResult> acc)
            => acc.Accept(this);
        public void Set(T key, int recnum)
        {
            base.Set((int)(object)key, recnum);
        }

        public WahBitArray QueryEquals(T key)
            => QueryEquals((int)(object)key);

        public WahBitArray QueryNotEquals(T key)
            => QueryNotEquals((int)(object)key);

        public bool GetFirst(T key, out int idx)
        {
            return base.GetFirst((int)(object)key, out idx);
        }
    }
}
