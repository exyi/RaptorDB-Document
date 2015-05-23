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
    public delegate T RowFill<T>(object[] data);
    public delegate object[] RowExtract<T>(T obj);
    static class ViewSchemaHelper<T>
    {
        public static readonly Type Type = typeof(T);
        public static readonly RowFill<T> RowFill = CreateRowFillerDelegate();
        public static RowFill<T> CreateRowFillerDelegate()
        {
            var objtype = Type;
            // TODO: use Linq.Expressions
            DynamicMethod dynMethod = new DynamicMethod("_", typeof(object), new Type[] { typeof(object), typeof(object[]) });
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
        }
    }

    static class ViewSchemaHelper {
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
    }
}
