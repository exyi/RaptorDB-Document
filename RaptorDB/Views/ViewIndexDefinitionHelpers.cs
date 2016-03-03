using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using RaptorDB.Common;
using RaptorDB.Views;
using System.Runtime.InteropServices;

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

        public static void SetMMIndex<TDoc, TSchema, TProp>(
            this View<TDoc, TSchema> view,
            System.Linq.Expressions.Expression<Func<TSchema, TProp>> selector,
            int pageSize = 8192,
            IPageSerializer<TProp> keySerializer = null)
            where TProp: IComparable<TProp>
        {
            var name = ExpressionHelper.GetPropertyName(selector);
            view.IndexDefinitions[name] = new MMIndexColumnDefinition<TProp>()
            {
                PageSize = pageSize,
                KeySerializer = keySerializer
            };
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

        public static void SetHashIndex<TDoc, TSchema, TProp>(
            this View<TDoc, TSchema> view,
            System.Linq.Expressions.Expression<Func<TSchema, TProp>> selector,
            long defaultSize = 4096,
            IPageSerializer<TProp> serializer = null)
        {
            var name = ExpressionHelper.GetPropertyName(selector);
            view.IndexDefinitions[name] = new HashIndexColumnDefinition<TProp>() { DefaultSize = defaultSize, KeySerializer = serializer };
        }

        public static void SetNoIndexing<TDoc, TSchema, TProp>(
            this View<TDoc, TSchema> view,
            System.Linq.Expressions.Expression<Func<TSchema, TProp>> selector)
        {
            var name = ExpressionHelper.GetPropertyName(selector);
            view.IndexDefinitions[name] = new NoIndexColumnDefinition();
        }

        public static IViewColumnIndexDefinition<T> GetDefaultForType<T>(bool allowDups = true)
        {
            if (typeof(T).IsValueType)
                return Activator.CreateInstance(typeof(MMIndexColumnDefinition<>).MakeGenericType(typeof(T)), new object[] { }) as IViewColumnIndexDefinition<T>;
            throw new NotImplementedException();
        }
    }
}
