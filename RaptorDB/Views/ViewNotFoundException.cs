using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace RaptorDB.Views
{
    public class ViewNotFoundException: Exception
    {
        public ViewNotFoundException(string viewName) : base(string.Format("view '{0}' was not found", viewName)) { }
    }
}
