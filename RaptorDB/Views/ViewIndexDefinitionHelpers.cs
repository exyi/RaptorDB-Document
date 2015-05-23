using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using RaptorDB.Common;
using RaptorDB.Views;

namespace RaptorDB
{
    public static class ViewIndexDefinitionHelpers
    {
        public static void SetStringIndex<TDoc, TSchema>(
            this View<TDoc, TSchema> view,
            System.Linq.Expressions.Expression<Func<TSchema, string>> selector,
            byte length = 60,
            bool ignoreCase = false)
        {
            var name = ExpressionHelper.GetPropertyName(selector);
            view.IndexDefinitions[name] = new StringIndexColumnDefinition(length);
        }

        public static void SetObjectToStringIndex<TDoc, TSchema, TProp>(
            this View<TDoc, TSchema> view,
            System.Linq.Expressions.Expression<Func<TSchema, TProp>> selector,
            byte length = 60,
            bool ignoreCase = false)
        {
            var name = ExpressionHelper.GetPropertyName(selector);
            view.IndexDefinitions[name] = new ObjectToStringColumnDefinition<TProp>(length);
        }

        public static void SetMGIndex<TDoc, TSchema, TProp>(
            this View<TDoc, TSchema> view,
            System.Linq.Expressions.Expression<Func<TSchema, TProp>> selector,
            byte length = 60)
            where TProp : struct, IComparable<TProp>
        {
            var name = ExpressionHelper.GetPropertyName(selector);
            view.IndexDefinitions[name] = new MGIndexColumnDefinition<TProp>(length);
        }

        public static void SetFullTextIndex<TDoc, TSchema>(
                this View<TDoc, TSchema> view,
                System.Linq.Expressions.Expression<Func<TSchema, string>> selector)
        {
            var name = ExpressionHelper.GetPropertyName(selector);
            view.IndexDefinitions[name] = new FullTextIndexColumnDefinition();
        }

        public static void SetEnumIndex<TDoc, TSchema, TProp>(
            this View<TDoc, TSchema> view,
            System.Linq.Expressions.Expression<Func<TSchema, TProp>> selector)
            where TProp : struct, IConvertible
        {
            var name = ExpressionHelper.GetPropertyName(selector);
            view.IndexDefinitions[name] = new EnumIndexColumnDefinition<TProp>();
        }

        public static void SetNoIndexing<TDoc, TSchema, TProp>(
            this View<TDoc, TSchema> view,
            System.Linq.Expressions.Expression<Func<TSchema, TProp>> selector)
        {
            var name = ExpressionHelper.GetPropertyName(selector);
            view.IndexDefinitions[name] = new NoIndexColumnDefinition();
        }
    }
}
