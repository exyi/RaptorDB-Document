using System;
using System.Collections.Generic;
using RaptorDB;
using RaptorDB.Views;


namespace SampleViews
{
    #region [  class definitions  ]
    //public enum State
    //{
    //    Open,
    //    Closed,
    //    Approved
    //}

    public class LineItem
    {
        public decimal QTY { get; set; }
        public string Product { get; set; }
        public decimal Price { get; set; }
        public decimal Discount { get; set; }
    }

    public class SalesInvoice
    {
        public SalesInvoice()
        {
            ID = Guid.NewGuid();
        }

        public Guid ID { get; set; }
        public string CustomerName { get; set; }
        public string NoCase { get; set; }
        public string Address { get; set; }
        public List<LineItem> Items { get; set; }
        public DateTime Date { get; set; }
        public int Serial { get; set; }
        public byte Status { get; set; }
        public bool Approved { get; set; }
        //public State InvoiceState { get; set; }
    }
    #endregion

    #region [  views  ]

    public class SalesInvoiceViewRowSchema : RDBSchema
    {
        //[FullText]
        public string CustomerName;
        [CaseInsensitive]
        [StringIndexLengthAttribute(255)]
        public string NoCase;
        public DateTime Date;
        public string Address;
        public int Serial;
        public byte Status;
        public bool Approved;
        //public State InvoiceState;
    }

    [RegisterView]
    public class SalesInvoiceView : View<SalesInvoice, SalesInvoiceViewRowSchema>
    {
        public SalesInvoiceView()
        {
            this.Name = "SalesInvoice";
            this.Description = "A primary view for SalesInvoices";
            this.isPrimaryList = true;
            this.isActive = true;
            this.BackgroundIndexing = true;
            this.Version = 6;
            //// uncomment the following for transaction mode
            //this.TransactionMode = true;

            this.SetFullTextIndex(s => s.CustomerName);
            this.SetFullTextIndex(s => s.Address);

            this.SetStringIndex(s => s.NoCase, length: 255, ignoreCase: true);

            this.Mapper = (api, docid, doc) =>
            {
                //int c = api.Count("SalesItemRows", "product = \"prod 1\"");
                if (doc.Serial == 0)
                    api.RollBack();
                api.EmitObject(docid, doc);
            };
        }
    }

    public class SalesItemRowsViewRowSchema : RDBSchema
    {
        public string Product;
        public decimal QTY;
        public decimal Price;
        public decimal Discount;
    }

    [RegisterView]
    public class SalesItemRowsView : View<SalesInvoice, SalesItemRowsViewRowSchema>
    {
        public SalesItemRowsView()
        {
            this.Name = "SalesItemRows";
            this.Description = "";
            this.isPrimaryList = false;
            this.isActive = true;
            this.BackgroundIndexing = true;

            this.Mapper = (api, docid, doc) =>
            {
                if (doc.Status == 3 && doc.Items != null)
                    foreach (var item in doc.Items)
                        api.EmitObject(docid, item);
            };
        }
    }

    public class NewViewRowSchema : RDBSchema
    {
        public string Product;
        public decimal QTY;
        public decimal Price;
        public decimal Discount;
    }

    [RegisterView]
    public class newview : View<SalesInvoice, NewViewRowSchema>
    {
        public newview()
        {
            this.Name = "newview";
            this.Description = "";
            this.isPrimaryList = false;
            this.isActive = true;
            this.BackgroundIndexing = true;
            this.Version = 1;

            this.Mapper = (api, docid, doc) =>
            {
                if (doc.Status == 3 && doc.Items != null)
                    foreach (var i in doc.Items)
                        api.EmitObject(docid, i);
            };
        }
    }
    #endregion
}
