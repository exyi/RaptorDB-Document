using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using RaptorDB.Common;

namespace RaptorDB
{
    /// <summary>
    /// Used to track ViewDelete usage for view rebuilds
    /// </summary>
    internal class View_delete
    {
        public Guid ID = Guid.NewGuid();
        public string Viewname;
        public string Filter;
    }

    internal class View_insert
    {
        public Guid ID = Guid.NewGuid();
        public string Viewname;
        public object RowObject;
    }


    internal interface IGetBytes<T>
    {
        byte[] GetBytes(T obj);
        T GetObject(byte[] buffer, int offset, int count);
    }


    internal class RDBDataType<T>
    {
        public static IGetBytes<T> ByteHandler()
        {
            Type type = typeof(T);

            if (type == typeof(int))            return (IGetBytes<T>)int_handler     .Instance;
            else if (type == typeof(uint))      return (IGetBytes<T>)uint_handler    .Instance;
            else if (type == typeof(long))      return (IGetBytes<T>)long_handler    .Instance;
            else if (type == typeof(Guid))      return (IGetBytes<T>)guid_handler    .Instance;
            else if (type == typeof(string))    return (IGetBytes<T>)string_handler  .Instance;
            else if (type == typeof(DateTime))  return (IGetBytes<T>)datetime_handler.Instance;
            else if (type == typeof(decimal))   return (IGetBytes<T>)decimal_handler .Instance;
            else if (type == typeof(short))     return (IGetBytes<T>)short_handler   .Instance;
            else if (type == typeof(float))     return (IGetBytes<T>)float_handler   .Instance;
            else if (type == typeof(byte))      return (IGetBytes<T>)byte_handler    .Instance;
            else if (type == typeof(double))    return (IGetBytes<T>)double_handler  .Instance;

            return null;
        }

        public static byte GetByteSize(byte keysize)
        {
            byte size = 4;
            Type t = typeof(T);

            if (t == typeof(int))      size = 4;
            if (t == typeof(uint))     size = 4;
            if (t == typeof(long))     size = 8;
            if (t == typeof(Guid))     size = 16;
            if (t == typeof(DateTime)) size = 8;
            if (t == typeof(decimal))  size = 16;
            if (t == typeof(float))    size = 4;
            if (t == typeof(short))    size = 2;
            if (t == typeof(string))   size = keysize;
            if (t == typeof(byte))     size = 1;
            if (t == typeof(double))   size = 8;

            return size;
        }

        internal static object GetEmpty()
        {
            Type t = typeof(T);

            if (t == typeof(string))
                return "";

            return default(T);
        }
    }

    #region [  handlers  ]

    internal class double_handler : IGetBytes<double>
    {
        public static double_handler Instance = new double_handler();
        public byte[] GetBytes(double obj)
        {
            return BitConverter.GetBytes(obj);
        }

        public double GetObject(byte[] buffer, int offset, int count)
        {
            return BitConverter.ToDouble(buffer, offset);
        }
    }

    internal class byte_handler : IGetBytes<byte>
    {
        public static byte_handler Instance = new byte_handler();
        public byte[] GetBytes(byte obj)
        {
            return new byte[1] { obj };
        }

        public byte GetObject(byte[] buffer, int offset, int count)
        {
            return buffer[offset];
        }
    }

    internal class float_handler : IGetBytes<float>
    {
        public static float_handler Instance = new float_handler();
        public byte[] GetBytes(float obj)
        {
            return BitConverter.GetBytes(obj);
        }

        public float GetObject(byte[] buffer, int offset, int count)
        {
            return BitConverter.ToSingle(buffer, offset);
        }
    }

    internal class decimal_handler : IGetBytes<decimal>
    {
        public static decimal_handler Instance = new decimal_handler();
        public byte[] GetBytes(decimal obj)
        {
            byte[] b = new byte[16];
            var bb = decimal.GetBits(obj);
            int index = 0;
            foreach (var d in bb)
            {
                byte[] db = Helper.GetBytes(d, false);
                Buffer.BlockCopy(db, 0, b, index, 4);
                index += 4;
            }

            return b;
        }

        public decimal GetObject(byte[] buffer, int offset, int count)
        {
            int[] i = new int[4];
            i[0] = Helper.ToInt32(buffer, offset);
            offset += 4;
            i[1] = Helper.ToInt32(buffer, offset);
            offset += 4;
            i[2] = Helper.ToInt32(buffer, offset);
            offset += 4;
            i[3] = Helper.ToInt32(buffer, offset);
            offset += 4;

            return new decimal(i);
        }
    }

    internal class short_handler : IGetBytes<short>
    {
        public static short_handler Instance = new short_handler();
        public byte[] GetBytes(short obj)
        {
            return Helper.GetBytes(obj, false);
        }

        public short GetObject(byte[] buffer, int offset, int count)
        {
            return Helper.ToInt16(buffer, offset);
        }
    }

    internal class string_handler : IGetBytes<string>
    {
        public static string_handler Instance = new string_handler();
        public byte[] GetBytes(string obj)
        {
            return Helper.GetBytes(obj);
        }

        public string GetObject(byte[] buffer, int offset, int count)
        {
            return Helper.GetString(buffer, offset, count);
        }
    }

    internal class int_handler : IGetBytes<int>
    {
        public static int_handler Instance = new int_handler();
        public byte[] GetBytes(int obj)
        {
            return Helper.GetBytes(obj, false);
        }

        public int GetObject(byte[] buffer, int offset, int count)
        {
            return Helper.ToInt32(buffer, offset);
        }
    }

    internal class uint_handler : IGetBytes<uint>
    {
        public static uint_handler Instance = new uint_handler();
        public byte[] GetBytes(uint obj)
        {
            return Helper.GetBytes(obj, false);
        }

        public uint GetObject(byte[] buffer, int offset, int count)
        {
            return (uint)Helper.ToInt32(buffer, offset);
        }
    }

    internal class long_handler : IGetBytes<long>
    {
        public static long_handler Instance = new long_handler();
        public byte[] GetBytes(long obj)
        {
            return Helper.GetBytes(obj, false);
        }

        public long GetObject(byte[] buffer, int offset, int count)
        {
            return Helper.ToInt64(buffer, offset);
        }
    }

    internal class guid_handler : IGetBytes<Guid>
    {
        public static guid_handler Instance = new guid_handler();
        public byte[] GetBytes(Guid obj)
        {
            return obj.ToByteArray();
        }

        public Guid GetObject(byte[] buffer, int offset, int count)
        {
            byte[] b = new byte[16];
            Buffer.BlockCopy(buffer, offset, b, 0, 16);
            return new Guid(b);
        }
    }

    internal class datetime_handler: IGetBytes<DateTime>
    {
        public static datetime_handler Instance = new datetime_handler();

        public byte[] GetBytes(DateTime obj)
        {
            return Helper.GetBytes(obj.Ticks, false);
        }

        public DateTime GetObject(byte[] buffer, int offset, int count)
        {
            long ticks = Helper.ToInt64(buffer, offset);

            return new DateTime(ticks);
        }
    }
    #endregion
}