using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace playground
{

    public class RRRandom : Random
    {
        int a;

        public RRRandom()
        {
            a = base.Next();
        }

        public override int Next()
        {
            unchecked
            {
                var ticks = (int)(DateTime.UtcNow.Ticks * 101701);
                var i = Interlocked.Increment(ref a);
                return ticks * i;
            }
        }

        public override int Next(int maxValue)
        {
            int bits = -1;
            var n = maxValue;
            while (n > 0)
            {
                n = n >> 1;
                bits++;
            }
            uint result;
            while (true)
            {
                result = ((uint)this.Next()) >> (32 - bits);
                if (result < maxValue) return (int)result;
            }
        }

        public override int Next(int minValue, int maxValue)
        {
            return Next(maxValue - minValue) + minValue;
        }

        public void HackToFaker()
        {
            var f = typeof(Faker.NumberFaker).GetField("_random", System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.NonPublic);
            f.SetValue(null, this);
        }
    }
}
