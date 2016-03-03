using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using RaptorDB.Indexes;
using RaptorDB.Common;

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

    public interface IViewColumnIndexDefinition<T>: IViewColumnIndexDefinition
    {
        new IIndex<T> CreateIndex(string path, string name);
    }
    public class MGIndexColumnDefinition : IViewColumnIndexDefinition
    {
        public Type Type { get; protected set; }
        public byte KeySize { get; protected set; }
        public bool AllowDuplicates { get; set; }
        public MGIndexColumnDefinition(Type type, byte keySize, bool allowDups = true)
        {
            Type = type; KeySize = keySize;
            AllowDuplicates = allowDups;
        }
        public MGIndexColumnDefinition(Type type)
            : this(type, (byte)Marshal.SizeOf(type))
        { }
        public virtual IIndex CreateIndex(string path, string name)
        {
            return (IIndex)Activator.CreateInstance(
                typeof(TypeIndexes<>).MakeGenericType(Type),
                new object[] { path, name, KeySize, AllowDuplicates });
        }
    }

    public class MGIndexColumnDefinition<T> : MGIndexColumnDefinition, IViewColumnIndexDefinition<T>
        where T : IComparable<T>
    {
        public MGIndexColumnDefinition(byte keySize) : base(typeof(T), keySize) { }
        public MGIndexColumnDefinition() : base(typeof(T)) { }
        public override IIndex CreateIndex(string path, string name)
        {
            return new TypeIndexes<T>(path, name, KeySize, AllowDuplicates);
        }

        IIndex<T> IViewColumnIndexDefinition<T>.CreateIndex(string path, string name)
        {
            return new TypeIndexes<T>(path, name, KeySize, AllowDuplicates);
        }
    }

    public class MMIndexColumnDefinition<T> : IViewColumnIndexDefinition<T>
        where T : IComparable<T>
    {
        public int PageSize { get; set; } = 8192;
        public IPageSerializer<T> KeySerializer { get; set; }
        public IIndex<T> CreateIndex(string path, string name)
        {
            return new MMIndex<T>(path, name, PageSize, KeySerializer);
        }

        IIndex IViewColumnIndexDefinition.CreateIndex(string path, string name)
        {
            return CreateIndex(path, name);
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

    public class EnumIndexColumnDefinition<T> : EnumIndexColumnDefinition, IViewColumnIndexDefinition<T>
        where T : struct, IConvertible
    {
        public EnumIndexColumnDefinition() : base(typeof(T)) { }

        public override IIndex CreateIndex(string path, string name)
        {
            return new EnumIntIndex<T>(path, name);
        }

        IIndex<T> IViewColumnIndexDefinition<T>.CreateIndex(string path, string name)
        {
            return new EnumIntIndex<T>(path, name);
        }
    }

    public class BoolIndexColumnDefinition : IViewColumnIndexDefinition
    {
        public IIndex CreateIndex(string path, string name)
        {
            return new BoolIndex(path, name, ".idx");
        }
    }

    public class StringIndexColumnDefinition : MGIndexColumnDefinition<string>
    {
        public StringIndexColumnDefinition(byte length) : base(length) { }
    }
    public class FullTextIndexColumnDefinition : IViewColumnIndexDefinition<string>
    {
        public IIndex<string> CreateIndex(string path, string name)
        {
            return new FullTextIndex(path, name, false, true);
        }

        IIndex IViewColumnIndexDefinition.CreateIndex(string path, string name)
        {
            return CreateIndex(path, name);
        }
    }

    public class ObjectToStringColumnDefinition<T> : IViewColumnIndexDefinition<T>
    {
        public ObjectToStringColumnDefinition(byte length)
        {
            this.MaxLength = length;
        }
        public byte MaxLength { get; set; }
        public IIndex<T> CreateIndex(string path, string name)
        {
            return new ObjectToStringIndex<T>(path, name, MaxLength);
        }

        IIndex IViewColumnIndexDefinition.CreateIndex(string path, string name)
        {
            return CreateIndex(path, name);
        }
    }

    public class HashIndexColumnDefinition<T> : IViewColumnIndexDefinition<T>
    {
        public long DefaultSize { get; set; } = 4096;
        public IPageSerializer<T> KeySerializer { get; set; }
        public IIndex<T> CreateIndex(string path, string name)
        {
            return new HashIndex<T>(path, name, DefaultSize, KeySerializer);
        }

        IIndex IViewColumnIndexDefinition.CreateIndex(string path, string name)
        {
            return CreateIndex(path, name);
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
