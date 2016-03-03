using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using RaptorDB.Common;

namespace RaptorDB
{
    public interface IResult
    {
        bool OK { get; }
        Exception EX { get; }
        int TotalCount { get; }
        int Count { get; }
        IList Rows { get; }
    }
    /// <summary>
    /// Result of queries
    ///    OK : T = Query with data,  F = EX has the exception
    ///    Rows : query rows
    /// </summary>
    public class Result<T>: IResult
    {
        public Result()
        {

        }
        public Result(bool ok)
        {
            OK = ok;
        }
        public Result(bool ok, Exception ex)
        {
            OK = ok;
            EX = ex;
        }
        /// <summary>
        /// T=Values return, F=exceptions occurred 
        /// </summary>
        public bool OK { get; set; }
        public Exception EX { get; set; }
        /// <summary>
        /// Total number of rows of the query
        /// </summary>
        public int TotalCount { get; set; }
        /// <summary>
        /// Rows returned
        /// </summary>
        public int Count { get; set; }

        IList IResult.Rows { get { return Rows; } }
        public List<T> Rows { get; set; }


        // FEATURE : data pending in results
        ///// <summary>
        ///// Data is being indexed, so results will not reflect all documents
        ///// </summary>
        //public bool DataPending { get; set; }
    }

    /// <summary>
    /// Base for row schemas : implements a docid property and is bindable
    /// </summary>
    public abstract class RDBSchema : BindableFields
    {
        public Guid docid;
    }
}
