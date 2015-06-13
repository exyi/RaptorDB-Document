using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml.Serialization;
using RaptorDB.Views;
using System.Reflection;

namespace RaptorDB
{
    public abstract class ViewBase
    {
        public delegate void MapFunctionDelgate<V>(IMapAPI api, Guid docid, V doc);
        /// <summary>
        /// Increment this when you change view definitions so the engine can rebuild the contents
        /// </summary>
        public int Version { get; set; }

        /// <summary>
        /// Name of the view will be used for foldernames and filename and generated code
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// A text for describing this views purpose for other developers 
        /// </summary>
        public string Description { get; set; }

        /// <summary>
        /// Column definitions for the view storage 
        /// </summary>
        public Type Schema { get; set; }

        /// <summary>
        /// Is this the primary list and will be populated synchronously
        /// </summary>
        public bool isPrimaryList { get; set; }

        /// <summary>
        /// Is this view active and will recieve data
        /// </summary>
        public bool isActive { get; set; }

        /// <summary>
        /// Delete items on DocID before inserting new rows (default = true)
        /// </summary>
        public bool DeleteBeforeInsert { get; set; }

        /// <summary>
        /// Index in the background : better performance but reads might not have all the data
        /// </summary>
        public bool BackgroundIndexing { get; set; }

        /// <summary>
        /// Save documents to this view in the save process, like primary views
        /// </summary>
        public bool ConsistentSaveToThisView { get; set; }

        /// <summary>
        /// Apply to a Primary View and all the mappings of all views will be done in a transaction.
        /// You can use Rollback for failures.
        /// </summary>
        public bool TransactionMode { get; set; }

        /// <summary>
        /// When defining your own schema and you don't want dependancies to RaptorDB to propogate through your code
        /// define your full text columns here
        /// </summary>
        [Obsolete("You should use IndexDefinitions and ViewIndexDefinitionHelper extension methods")]
        public List<string> FullTextColumns = new List<string>();

        /// <summary>
        /// When defining your own schems and you don't want dependancies to RaptorDB to propogate through your code 
        /// define your case insensitive columns here
        /// </summary>
        [Obsolete("You should use IndexDefinitions and ViewIndexDefinitionHelper extension methods")]
        public List<string> CaseInsensitiveColumns = new List<string>();


        [Obsolete("You should use IndexDefinitions and ViewIndexDefinitionHelper extension methods")]
        public Dictionary<string, byte> StringIndexLength = new Dictionary<string, byte>();

        /// <summary>
        /// Columns that you don't want to index
        /// </summary>
        [Obsolete("You should use IndexDefinitions and ViewIndexDefinitionHelper extension methods")]
        public List<string> NoIndexingColumns = new List<string>();

        public Dictionary<string, IViewColumnIndexDefinition> IndexDefinitions { get; set; }


        public void AutoInitIndexDefinitions()
        {
            foreach (var p in Schema.GetProperties())
            {
                if (!IndexDefinitions.ContainsKey(p.Name))
                {
                    Type t = p.PropertyType;
                    IndexDefinitions[p.Name] = AutoInitMember(p, t);
                }
            }

            foreach (var f in Schema.GetFields())
            {
                if (!IndexDefinitions.ContainsKey(f.Name))
                {
                    Type t = f.FieldType;
                    IndexDefinitions[f.Name] = AutoInitMember(f, t);
                }
            }
        }

#pragma warning disable CS0618 // Type or member is obsolete
        public IViewColumnIndexDefinition AutoInitMember(MemberInfo p, Type t)
        {
            if (NoIndexingColumns.Contains(p.Name) || NoIndexingColumns.Contains(p.Name.ToLower()))
            {
                return new NoIndexColumnDefinition();
            }
            else
            {
                if (FullTextColumns.Contains(p.Name) || FullTextColumns.Contains(p.Name.ToLower()) || p.GetCustomAttributes(typeof(FullTextAttribute), true).Length > 0)
                    return new FullTextIndexColumnDefinition();

                var cs = p.GetCustomAttributes(typeof(CaseInsensitiveAttribute), true).Length > 0 ||
                    CaseInsensitiveColumns.Contains(p.Name) || CaseInsensitiveColumns.Contains(p.Name.ToLower());

                byte length = Global.DefaultStringKeySize;
                var a = p.GetCustomAttributes(typeof(StringIndexLengthAttribute), false);
                if (a.Length > 0)
                {
                    length = (a[0] as StringIndexLengthAttribute).Length;
                }
                if (StringIndexLength.ContainsKey(p.Name) || StringIndexLength.ContainsKey(p.Name.ToLower()))
                {
                    if (!StringIndexLength.TryGetValue(p.Name, out length))
                        StringIndexLength.TryGetValue(p.Name.ToLower(), out length);
                }

                if (t == typeof(string))
                {
                    // TODO: case sensitive index
                    return new StringIndexColumnDefinition(length);
                }
                else if (t == typeof(bool))
                {
                    return new BoolIndexColumnDefinition();
                }
                return new MGIndexColumnDefinition(t, length);
            }
        }
#pragma warning restore CS0618 // Type or member is obsolete

        public abstract Type GetDocType();
    }


    public class View<T> : ViewBase
    {
        public View()
        {
            isActive = true;
            DeleteBeforeInsert = true;
            BackgroundIndexing = true;
            IndexDefinitions = new Dictionary<string, IViewColumnIndexDefinition>()
            {
                {"docid", new MGIndexColumnDefinition<Guid>(16) }
            };
        }

        /// <summary>
        /// Inline delegate for the mapper function used for quick applications 
        /// </summary>
        [XmlIgnore]
        public MapFunctionDelgate<T> Mapper { get; set; }

        public void Verify()
        {
            if (string.IsNullOrEmpty(this.Name))
                throw new Exception("Name must be given");
            if (Schema == null)
                throw new Exception("Schema must be defined");
            if (Schema.IsSubclassOf(typeof(RDBSchema)) == false)
            {
                var pi = Schema.GetProperty("docid");
                if (pi == null || pi.PropertyType != typeof(Guid))
                {
                    var fi = Schema.GetField("docid");
                    if (fi == null || fi.FieldType != typeof(Guid))
                        throw new Exception("The schema must be derived from RaptorDB.RDBSchema or must contain a 'docid' Guid field or property");
                }
            }
            if (Mapper == null)
                throw new Exception("A map function must be defined");

            if (TransactionMode == true && isPrimaryList == false)
                throw new Exception("Transaction mode can only be enabled on Primary Views");

            // FEATURE : add more verifications
        }

        public override Type GetDocType()
        {
            return typeof(T);
        }
    }

    public abstract class View<TDoc, TSchema> : View<TDoc>
    {
        public View()
        {
            this.Schema = typeof(TSchema);
        }
    }
}
