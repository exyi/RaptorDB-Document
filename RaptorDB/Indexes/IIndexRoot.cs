using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RaptorDB.Indexes
{
    interface IIndexRoot<in TKey>
    {
        int GetPageIndex(TKey key);
        IEnumerable<int> GetLowerPagesIndexes(int index);
        IEnumerable<int> GetUpperPagesIndexes(int index);
        int CreateTable(TKey firstKey);
    }
}
