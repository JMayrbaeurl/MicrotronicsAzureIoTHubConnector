using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace AddTests
{
    class Program
    {
        static void Main(string[] args)
        {
            IoTHubSendTest();
        }

        public static void IoTHubSendTest()
        {
            WebClient webClient = new WebClient();
            webClient.Headers.Add("user-agent",
                   "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.2; .NET CLR 1.0.3705;)");

            webClient.Headers.Add("Authorization", "[REPLACE]");

            string finalUrl = "https://zeppelintst.azure-devices.net/devices/rapidM2MForwarder/messages/events?api-version=2016-11-14";
            using (Stream data = webClient.OpenWrite(finalUrl, "POST"))
            {
                string postData = @"{""object"":{""name"":""Name""}}";
                byte[] postArray = Encoding.ASCII.GetBytes(postData);
                try
                {
                    data.Write(postArray, 0, postArray.Length);
                } catch (System.Net.WebException ex )
                {
                    System.Console.WriteLine($"Exception: {ex.Message}");
                }
             }
        }

        public static void ParseTest()
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
