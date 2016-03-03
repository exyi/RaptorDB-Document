using RaptorDB.Common;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace RaptorDB
{
    public enum RDBExpression
    {
        Equal,
        Greater,
        GreaterEqual,
        Less,
        LessEqual,
        NotEqual,
        Between,
        Contains
    }

    public interface IIndexAcceptable<out TResult>
    {
        TResult Accept<T>(IIndex<T> item);
    }

    public interface IIndex: IDisposable
    {
        void FreeMemory();
        void SaveIndex();
        TResult Accept<TResult>(IIndexAcceptable<TResult> acc);
        void Set(object key, int recnum);
        bool AllowsDuplicates { get; }
    }

    public interface IIndex<T>: IIndex
    {
        void Set(T key, int recnum);
        T[] GetKeys();
    }

    public interface IUpdatableIndex<T>: IIndex<T>
    {
        bool Remove(T key);
        bool Remove(T key, int recnum);
        void ReplaceFirst(T key, int recnum);
        void Replace(T key, int oldNum, int newNum);
    }

    public interface IEqualsQueryIndex<T>: IIndex<T>
    {
        WahBitArray QueryEquals(T key);
        WahBitArray QueryNotEquals(T key);
        bool GetFirst(T key, out int idx);
    }

    public interface IComparisonIndex<T>: IEqualsQueryIndex<T>
    {
        WahBitArray QueryGreater(T key);
        WahBitArray QueryGreaterEquals(T key);
        WahBitArray QueryLess(T key);
        WahBitArray QueryLessEquals(T key);
    }

    public interface IBetweenComparisonIndex<T>: IEqualsQueryIndex<T>
    {
        WahBitArray QueryBetween(T from, T to);
    }

    public interface IContainsIndex<T>: IIndex<T>
    {
        WahBitArray QueryContains(T value);
    }
}
