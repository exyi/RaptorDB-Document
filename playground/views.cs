using RaptorDB;
using RaptorDB.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace playground
{
    public class ModelItem
    {
        public string Name { get; set; }
        public int Number { get; set; }
        public string WebSite { get; set; }
        public Guid Id { get; set; }
        public string[] Friends { get; set; }

        public ModelItem()
        {
            Friends = new string[0];
        }
    }

    public class OtherItem
    {
        public int[] Numbers;
        public Dictionary<string, byte[]> Bytes;
    }

    public class FriendsModelItemView : View<ModelItem, FriendsModelItemView.RowSchema>
    {
        public class RowSchema : RDBSchema
        {
            public string Name;
        }
        public FriendsModelItemView()
        {
            this.Name = "friends";
            this.Description = "ModelItems friens";
            this.ConsistentSaveToThisView = true;
            this.isActive = true;
            this.BackgroundIndexing = false;
            this.Version = 1;
            // // uncomment the following for transaction mode
            //this.TransactionMode = true;

            this.SetStringIndex(s => s.Name, length: 255);
            //this.SetMMIndex(s => s.Name, keySerializer: new PageHashTableHelper.StringPageSerializer(255));

            this.Mapper = (api, docid, doc) =>
            {
                foreach (var f in doc.Friends)
                {
                    api.Emit(docid, f);
                }
            };
        }

    }

    public class DefaultModelItemView : RaptorDB.View<ModelItem, DefaultModelItemView.RowSchema>
    {
        public class RowSchema : RDBSchema
        {
            public string Name;
            public int Number;
        }
        public DefaultModelItemView()
        {
            this.Name = "ModelItem";
            this.Description = "A primary view for ModelItem";
            this.isPrimaryList = true;
            this.isActive = true;
            this.BackgroundIndexing = false;
            this.Version = 1;
            //// uncomment the following for transaction mode
            //this.TransactionMode = true;

            this.SetStringIndex(s => s.Name, ignoreCase: true);
            this.SetMMIndex(s => s.Number);

            this.Mapper = (api, docid, doc) =>
            {
                api.EmitObject(docid, doc);
            };
        }
    }
}
