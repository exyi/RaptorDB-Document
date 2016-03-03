using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace RaptorDB.Common
{
    public static class RWLockExtensions
    {
        class ReadingDisposableLock : IDisposable
        {
            ReaderWriterLockSlim rwl;
            public ReadingDisposableLock(ReaderWriterLockSlim rwl) {
                this.rwl = rwl;
                this.rwl.EnterReadLock();
            }
            public void Dispose()
            {
                rwl.ExitReadLock();
            }
        }
        class WritingDisposableLock : IDisposable
        {
            ReaderWriterLockSlim rwl;
            public WritingDisposableLock(ReaderWriterLockSlim rwl)
            {
                this.rwl = rwl;
                this.rwl.EnterWriteLock();
            }
            public void Dispose()
            {
                rwl.ExitWriteLock();
            }
        }
        class UpgradeableReadingDisposableLock : IDisposable
        {
            ReaderWriterLockSlim rwl;
            public UpgradeableReadingDisposableLock(ReaderWriterLockSlim rwl)
            {
                this.rwl = rwl;
                this.rwl.EnterUpgradeableReadLock();
            }
            public void Dispose()
            {
                rwl.ExitUpgradeableReadLock();
            }
        }
        public static IDisposable Reading(this ReaderWriterLockSlim rwl)
        {
            return new ReadingDisposableLock(rwl);
        }
        public static IDisposable UpgradeableReading(this ReaderWriterLockSlim rwl)
        {
            return new UpgradeableReadingDisposableLock(rwl);
        }
        public static IDisposable Writing(this ReaderWriterLockSlim rwl)
        {
            return new WritingDisposableLock(rwl);
        }
    }
}
