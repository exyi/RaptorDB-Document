using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

namespace RaptorDB.Common
{
    public static class ExpressionHelper
    {
        /// <summary>
        /// Gets name of property in member expression
        /// </summary>
        public static string GetPropertyName<TObj, TProp>(System.Linq.Expressions.Expression<Func<TObj, TProp>> lambda)
        {
            var m = lambda.Body as MemberExpression;
            if (m == null) throw new ArgumentException("lambda is not MemberExpression");
            return m.Member.Name;
        }
    }
}
