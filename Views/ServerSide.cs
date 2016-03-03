using System;
using System.Collections.Generic;
using System.Linq;
using RaptorDB.Common;
using SampleViews;
using RaptorDB;

namespace Views
{
    public class ServerSide
    {
        // so the result can be serialized and is not an anonymous type
        // since this uses fields, derive from the BindableFields for data binding to work
        public class sumtype : BindableFields
        {
            public string Product;
            public decimal TotalPrice;
            public decimal TotalQTY;
        }

        public static List<object> Sum_Products_based_on_filter(IRaptorDB rap, string filter)
        {
            var q = rap.Query<SalesItemRowsViewRowSchema>(filter);

            var res = from x in q.Rows
                      group x by x.Product into g
                      select new sumtype // avoid anonymous types
                      {
                          Product = g.Key,
                          TotalPrice = g.Sum(p => p.Price),
                          TotalQTY = g.Sum(p => p.QTY)
                      };

            return res.ToList<object>();
        }

        public static List<sumtype> DoServerSideSumOnRaptor(IRaptorDB rap, string productName)
        {
            return rap.ServerSide((r, f) =>
            {
                var q = r.Query<SalesItemRowsViewRowSchema>(i => i.Product == productName);
                var res = from x in q.Rows
                          group x by x.Product into g
                          select new sumtype
                          {
                              Product = g.Key,
                              TotalPrice = g.Sum(p => p.Price),
                              TotalQTY = g.Sum(p => p.QTY)
                          };
                return res.ToList<object>();
            }, null).Cast<sumtype>().ToList();
        }
    }
}
