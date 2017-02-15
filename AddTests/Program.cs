using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AddTests
{
    class Program
    {
        static void Main(string[] args)
        {
            string response = "[[\"20170126133\", 51000,\"NAN\"]]";

            List<List<String>> values = JsonConvert.DeserializeObject<List<List<String>>>(response);
            if (values != null && values.Count > 0)
            {

            }

            string timeStampString = "20170126133".PadRight(17, '0');

            DateTime timestamp = 
                DateTime.ParseExact(timeStampString, "yyyyMMddHHmmssfff", CultureInfo.InvariantCulture);
            if (timestamp != null)
            { }
        }
    }
}
