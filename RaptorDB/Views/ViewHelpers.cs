using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Reflection.Emit;
using System.Text;
using fastJSON;

namespace RaptorDB.Views
{
    internal delegate T RowFill<T>(object[] data);
    public delegate object[] RowExtract<T>(T obj);
    static class ViewSchemaHelper<T>
    {
        public static readonly Type Type = typeof(T);
        [Obsolete]
        public static readonly RowFill<T> RowFill = CreateRowFillerDelegate();
        [Obsolete]
        public static RowFill<T> CreateRowFillerDelegate()
        {
            var objtype = Type;
            // TODO: use Linq.Expressions
            DynamicMethod dynMethod = new DynamicMethod("_", objtype, new Type[] { typeof(object[]) });
            ILGenerator il = dynMethod.GetILGenerator();
            var local = il.DeclareLocal(objtype);
            il.Emit(OpCodes.Newobj, objtype.GetConstructor(Type.EmptyTypes));
            il.Emit(OpCodes.Castclass, objtype);
            il.Emit(OpCodes.Stloc, local);
            int i = 1;

            foreach (var c in objtype.GetFields())
            {
                il.Emit(OpCodes.Ldloc, local);
                il.Emit(OpCodes.Ldarg_1);
                if (c.Name != "docid")
                    il.Emit(OpCodes.Ldc_I4, i++);
                else
                    il.Emit(OpCodes.Ldc_I4, 0);

                il.Emit(OpCodes.Ldelem_Ref);
                il.Emit(OpCodes.Unbox_Any, c.FieldType);
                il.Emit(OpCodes.Stfld, c);
            }

            foreach (var c in objtype.GetProperties())
            {
                MethodInfo setMethod = c.GetSetMethod();
                il.Emit(OpCodes.Ldloc, local);
                il.Emit(OpCodes.Ldarg_1);
                if (c.Name != "docid")
                    il.Emit(OpCodes.Ldc_I4, i++);
                else
                    il.Emit(OpCodes.Ldc_I4, 0);
                il.Emit(OpCodes.Ldelem_Ref);
                il.Emit(OpCodes.Unbox_Any, c.PropertyType);
                il.EmitCall(OpCodes.Callvirt, setMethod, null);
            }

            il.Emit(OpCodes.Ldloc, local);
            il.Emit(OpCodes.Ret);

            return (RowFill<T>)dynMethod.CreateDelegate(typeof(RowFill<T>));

            //var objtype = Type;
            //// TODO: use Linq.Expressions
            //var 
            //var statements = new List<Expression>();
            //ILGenerator il = dynMethod.GetILGenerator();
            //var local = il.DeclareLocal(objtype);
            //il.Emit(OpCodes.Newobj, objtype.GetConstructor(Type.EmptyTypes));
            //il.Emit(OpCodes.Castclass, objtype);
            //il.Emit(OpCodes.Stloc, local);
            //int i = 1;

            //foreach (var c in objtype.GetFields())
            //{
            //    il.Emit(OpCodes.Ldloc, local);
            //    il.Emit(OpCodes.Ldarg_1);
            //    if (c.Name != "docid")
            //        il.Emit(OpCodes.Ldc_I4, i++);
            //    else
            //        il.Emit(OpCodes.Ldc_I4, 0);

            //    il.Emit(OpCodes.Ldelem_Ref);
            //    il.Emit(OpCodes.Unbox_Any, c.FieldType);
            //    il.Emit(OpCodes.Stfld, c);
            //}

            //foreach (var c in objtype.GetProperties())
            //{
            //    MethodInfo setMethod = c.GetSetMethod();
            //    il.Emit(OpCodes.Ldloc, local);
            //    il.Emit(OpCodes.Ldarg_1);
            //    if (c.Name != "docid")
            //        il.Emit(OpCodes.Ldc_I4, i++);
            //    else
            //        il.Emit(OpCodes.Ldc_I4, 0);
            //    il.Emit(OpCodes.Ldelem_Ref);
            //    il.Emit(OpCodes.Unbox_Any, c.PropertyType);
            //    il.EmitCall(OpCodes.Callvirt, setMethod, null);
            //}

            //il.Emit(OpCodes.Ldloc, local);
            //il.Emit(OpCodes.Ret);

            //return (RowFill<T>)dynMethod.CreateDelegate(typeof(RowFill<T>));
        }
    }

    static class ViewSchemaHelper
    {
        public static List<object[]> ExtractRows(List<object> rows, string[] columnNames)
        {
            // TODO: precompile this like RowFiller
            List<object[]> output = new List<object[]>();
            // reflection match object properties to the schema row
            var colcount = columnNames.Length;
            foreach (var obj in rows)
            {
                object[] r = new object[colcount];
                Getters[] getters = Reflection.Instance.GetGetters(obj.GetType(), true, null);

                for (int i = 0; i < colcount; i++)
                {
                    var c = columnNames[i];
                    foreach (var g in getters)
                    {
                        if (g.Name == c)
                        {
                            r[i] = g.Getter(obj);
                            break;
                        }
                    }
                }
                output.Add(r);
            }

            return output;
        }

        public static RowFill<T> CreateRowFiller<T>(string[] columns)
        {
            var values = Expression.Parameter(typeof(object[]), "columns");
            var row = Expression.Variable(typeof(T), "row");
            var block = new List<Expression>();

            if (typeof(T).IsClass)
                block.Add(Expression.Assign(row, Expression.New(typeof(T))));

            block.Add(ConvertAndAssign(row, "docid",
                Expression.ArrayIndex(values, Expression.Constant(0))));
            int i = 1;
            foreach (var col in columns)
            {
                block.Add(ConvertAndAssign(row, col,
                    Expression.ArrayIndex(values, Expression.Constant(i))));
                i++;
            }

            // return row;
            block.Add(row);

            return Expression.Lambda<RowFill<T>>(Expression.Block(typeof(T), new[] { row }, block), values).Compile();
        }

        private static Expression ConvertAndAssign(Expression obj, string propName, Expression value)
        {
            // TODO: generic property API
            var property = obj.Type.GetProperty(propName);
            if (property != null)
            {
                return Expression.Assign(
                    Expression.Property(obj, property),
                    Expression.Convert(value, property.PropertyType));
            }
            var field = obj.Type.GetField(propName);
            if (field != null)
            {
                return Expression.Assign(
                    Expression.Field(obj, field),
                    Expression.Convert(value, field.FieldType));
            }
            throw new ArgumentException("specified property does not exist");
        }
    }
}
