using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO.MemoryMappedFiles;
using RaptorDB.Common;

namespace RaptorDB.Indexes
{
    public unsafe class MemoryMappedIndexFile<TKey, TValue>
    {
        MemoryMappedFile file;
        MemoryMappedViewAccessor fileAccessor;

    }
}
