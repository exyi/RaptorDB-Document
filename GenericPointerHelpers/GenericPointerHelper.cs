using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GenericPointerHelpers
{
    public unsafe static class GenericPointerHelper
    {
        public static T Read<T>(void* ptr)
        {
            throw new NotImplementedException();
        }

        public static void Write<T>(void* ptr, T value)
        {
            throw new NotImplementedException();
        }
    }
}
