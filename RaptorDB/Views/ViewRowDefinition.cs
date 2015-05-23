using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using RaptorDB.Indexes;

namespace RaptorDB.Views
{

    public class ViewRowDefinition
    {
        public ViewRowDefinition()
        {
            Columns = new List<KeyValuePair<string, IViewColumnIndexDefinition>>();
        }
        public string Name { get; set; }
        public List<KeyValuePair<string, IViewColumnIndexDefinition>> Columns { get; set; }

        public void Add(string name, IViewColumnIndexDefinition type)
        {
            Columns.Add(new KeyValuePair<string, IViewColumnIndexDefinition>(name, type));
        }
    }

    public interface IViewColumnIndexDefinition
    {
        IIndex CreateIndex(string path, string name);
    }
    public class MGIndexColumnDefinition : IViewColumnIndexDefinition
    {
        public Type Type { get; protected set; }
        public byte KeySize { get; protected set; }
        public MGIndexColumnDefinition(Type type, byte keySize)
        {
            Type = type; KeySize = keySize;
        }
        public MGIndexColumnDefinition(Type type)
            : this(type, (byte)Marshal.SizeOf(type))
        { }
        public virtual IIndex CreateIndex(string path, string name)
        {
            return (IIndex)Activator.CreateInstance(
                typeof(TypeIndexes<>).MakeGenericType(Type),
                new object[] { path, name, KeySize });
        }
    }

    public class MGIndexColumnDefinition<T> : MGIndexColumnDefinition
        where T : IComparable<T>
    {
        public MGIndexColumnDefinition(byte keySize) : base(typeof(T), keySize) { }
        public MGIndexColumnDefinition() : base(typeof(T)) { }
        public override IIndex CreateIndex(string path, string name)
        {
            return new TypeIndexes<T>(path, name, KeySize);
        }
    }

    public class EnumIndexColumnDefinition : IViewColumnIndexDefinition
    {
        public Type Type { get; protected set; }
        public EnumIndexColumnDefinition(Type type)
        {
            this.Type = type;
        }
        public virtual IIndex CreateIndex(string path, string name)
        {
            return (IIndex)Activator.CreateInstance(
                typeof(EnumIntIndex<>).MakeGenericType(Type),
                new object[] { path, name });
        }
    }

    public class EnumIndexColumnDefinition<T>: EnumIndexColumnDefinition
        where T: struct, IConvertible
    {
        public EnumIndexColumnDefinition() : base(typeof(T)) { }

        public override IIndex CreateIndex(string path, string name)
        {
            return new EnumIntIndex<T>(path, name);
        }
    }

    public class BoolColumnIndex<T> : IViewColumnIndexDefinition
    {
        public IIndex CreateIndex(string path, string name)
        {
            return new BoolIndex(path, name, ".idx");
        }
    }

    public class StringIndexColumnDefinition: MGIndexColumnDefinition<string>
    {
        public StringIndexColumnDefinition(byte length) : base(length) { }
    }
    public class FullTextIndexColumnDefinition : IViewColumnIndexDefinition
    {
        public IIndex CreateIndex(string path, string name)
        {
            return new FullTextIndex(path, name, false, true);
        }
    }

    public class ObjectToStringColumnDefinition<T> : IViewColumnIndexDefinition
    {
        public ObjectToStringColumnDefinition(byte length)
        {
            this.MaxLength = length;
        }
        public byte MaxLength { get; set; }
        public IIndex CreateIndex(string path, string name)
        {
            return new ObjectToStringIndex<T>(path, name, MaxLength);
        }
    }
    public class NoIndexColumnDefinition : IViewColumnIndexDefinition
    {
        public IIndex CreateIndex(string path, string name)
        {
            return NoIndex.Instance;
        }
    }
}
