#load ".\models\channelvalues.csx"
#load ".\models\multipledatapoints.csx"
#load ".\models\pollingattempt.csx"
#load ".\iothubclient.csx"
#load ".\dataaccess.csx"

using System;
using System.Collections.Generic;
using System.Net;
using System.Configuration;
using System.Globalization;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Newtonsoft.Json;

public static void Run(TimerInfo myTimer, TraceWriter log)
{
    using (M2MBackendClient client = CreateM2MBackendClient(log))
    {
        foreach (string siteId in client.Site_Ids)
        {
            List<ChannelValues> values = null;
            DateTime pollingTime = DateTime.Now;
            client.FindLastPollingEntry(siteId, log);

            if (!client.IsEmulating)
            {
                // Get latest not processed values from m2mBackend service
                values = client.GetNewTelemetryData(siteId, myTimer, log);

                if (values != null)
                    log.Info($"Calling m2mBackend returned {values.Count} channel values");
            }
            else
            {
                ChannelValues aValue = new ChannelValues(DateTime.Now);
                aValue.AddChannelValue(client.ChannelNames[0],
                    (int)(DateTime.Now - new DateTime(2017, 1, 1)).TotalSeconds);
                values = new List<ChannelValues>();
                values.Add(aValue);

                log.Info($"Generated random values for channels");
            }

            PollingAttempt polling = client.CreatePollingAttemptForChannelValues(siteId, pollingTime, values);

            // If we have got data, forward them to IoT Hub
            if (values != null && values.Count > 0)
            {
                using (IoTHubSender iothubclient = CreateIoTHubSender(log))
                {
                    MultipleDatapoints datapoints = CreateDatapointsFromChannelValues(client, siteId, pollingTime, values);
                    if (datapoints != null)
                    {
                        iothubclient.SendDataFrame(datapoints, log);
                        log.Info($"Successfully forwarded data points to IoT Hub");
                    }

                    if (polling != null)
                        polling.IoTHubMessage = datapoints;
                }
            }

            // And log polling attempt to DocDB
            if (polling != null)
                client.WritePollingAttemptToDB(polling, log).Wait();
        }
    }

    log.Info($"C# Timer trigger function executed at: {DateTime.Now}");    
}

private static M2MBackendClient CreateM2MBackendClient(TraceWriter log)
{
    string m2mBackendURL = System.Configuration.ConfigurationManager.AppSettings.Get("M2MBackendURL");
    string authorizationString = System.Configuration.ConfigurationManager.AppSettings.Get("Base64encodedUsernamePassword");
    string m2mDBUri = System.Configuration.ConfigurationManager.AppSettings.Get("M2MDocumentsDBUri");
    string m2mDBKey = System.Configuration.ConfigurationManager.AppSettings.Get("M2MDocumentsDBPrimaryKey");
    string emulModeString = System.Configuration.ConfigurationManager.AppSettings.Get("EmulationMode");

    M2MBackendClient result = new M2MBackendClient(m2mBackendURL, authorizationString, m2mDBUri, m2mDBKey);
    result.SetIdentification(System.Configuration.ConfigurationManager.AppSettings.Get("Customer_ID"),
        System.Configuration.ConfigurationManager.AppSettings.Get("Site_ID"));

    result.CreateDatabaseIfNotExists(log).Wait();
    result.CreatePollingHistoryCollectionIfNotExists(log).Wait();

    result.IsEmulating = Boolean.TrueString.ToLower().Equals(emulModeString.ToLower());

    return result;
}

private static IoTHubSender CreateIoTHubSender(TraceWriter log)
{
    string connString = System.Configuration.ConfigurationManager.AppSettings.Get("IoTHubConnection");
    IoTHubSenderFactory factory = null;
    if (connString != null)
        factory = new IoTHubSenderFactory() { ConnectionString = connString };
    else
    {
        string ioTHubURL = System.Configuration.ConfigurationManager.AppSettings.Get("IoTHubURL");
        string ioTHubDeviceId = System.Configuration.ConfigurationManager.AppSettings.Get("IoTHubDeviceId");
        string ioTHubSharedAccessSignature = System.Configuration.ConfigurationManager.AppSettings.Get("IoTHubSharedAccessSignature");

        factory = new IoTHubSenderFactory() { IoTHubURL = ioTHubURL,
            IotHubDeviceId = ioTHubDeviceId, IotHubSASignature = ioTHubSharedAccessSignature
        };
    }

    return factory.CreateNewInstance(log);
}

private static MultipleDatapoints CreateDatapointsFromChannelValues(M2MBackendClient m2mBackendClient, string siteId, DateTime pollingTime, List<ChannelValues> values)
{
    MultipleDatapoints datapoints = new MultipleDatapoints();

    datapoints.customer_id = m2mBackendClient.Customer_Id;
    datapoints.site_id = siteId;

    datapoints.Timerange = m2mBackendClient.CreateCurrentTimerange(pollingTime, values);

    datapoints.Timeseries = new List<TimeseriesEntry>();
    foreach (ChannelValues value in values )
    {
        TimeseriesEntry entry = new TimeseriesEntry(value.Timestamp);
        foreach (KeyValuePair<string, int> channelEntry in value.Values)
        {
            entry.Channels.Add(channelEntry.Key, channelEntry.Value);
        }

        datapoints.Timeseries.Add(entry);
    }

    return datapoints;
}

public class M2MBackendClient : IDisposable
{
    private string url = "https://poc.microtronics.at/api/1/";

    private string customer_id = null;
    public string Customer_Id { get { return this.customer_id; } }

    private string[] site_ids = null;
    public string[] Site_Ids { get { return this.site_ids; } }

    private List<String> channelList = new List<String>() { "ch0", "ch3", "ch1" };
    public List<String> ChannelNames { get { return this.channelList;  } }

    private string databaseName = "M2MBackendForwarderData";
    private string collectionName = "pollingAttempts";

    private WebClient webClient = null;
    private PollingDB pollingDB = null;

    private PollingAttempt lastForward = null;

    public bool IsEmulating { get; set; }
 
    public M2MBackendClient()
    {
        this.webClient = new WebClient();
        this.webClient.Headers.Add("user-agent",
               "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.2; .NET CLR 1.0.3705;)");
    }

    public M2MBackendClient(string url, string usernamePassword, string dbUri, string dbKey) : this()
    {
        this.url = url;

        this.webClient.Headers.Add("Authorization", $"Basic {usernamePassword}");
        this.pollingDB = PollingDB.CreateInstance(this.databaseName, this.collectionName, dbUri, dbKey);
    }

    public void SetIdentification(string custId, string siteId)
    {
        this.customer_id = custId;
        this.site_ids = siteId.Split(new Char[] { ','});
    }

    public async Task CreateDatabaseIfNotExists(TraceWriter log)
    {
        if (this.pollingDB != null)
            await this.pollingDB.CreateDatabaseIfNotExists(log);
    }

    public async Task CreatePollingHistoryCollectionIfNotExists(TraceWriter log)
    {
        if (this.pollingDB != null)
            await this.pollingDB.CreatePollingHistoryCollectionIfNotExists(log);
    }

    public void FindLastPollingEntry(string siteId, TraceWriter log)
    {
        if (this.pollingDB != null)
            this.lastForward = this.pollingDB.FindLastPollingEntry(this.Customer_Id, siteId, log);
    }

    public async Task WritePollingAttemptToDB(PollingAttempt polling, TraceWriter log)
    {
        if (this.pollingDB != null)
            await this.pollingDB.WritePollingAttemptToDB(polling, log);
    }

    public PollingAttempt CreatePollingAttemptForChannelValues(string siteId, DateTime pollingTime, List<ChannelValues> values)
    {
        PollingAttempt polling = new PollingAttempt();
        polling.PollingTimestamp = pollingTime;
        polling.Customer_Id = this.customer_id;
        polling.Site_Id = siteId;

        TimeSpan timeSpanSince2017 = polling.PollingTimestamp - new DateTime(2017, 1, 1);
        polling.Id = timeSpanSince2017.TotalMilliseconds.ToString();
        polling.M2MData = values;

        return polling;
    }

    public string GetMe(TimerInfo myTimer, TraceWriter log)
    {
        string result = "";

        Stream data = this.webClient.OpenRead(this.url + "me");
        StreamReader reader = new StreamReader(data);
        result = reader.ReadToEnd();

        data.Close();
        reader.Close();

        return result;
    }

    public List<ChannelValues> GetNewTelemetryData(string siteId, TimerInfo myTimer, TraceWriter log)
    {
        List<ChannelValues> result = new List<ChannelValues>();

        try
        {
            string finalUrl = this.url +
                $"customers/{this.customer_id}/sites/{siteId}/histdata0" +
                (this.lastForward != null ? "" : "/youngest") +
                "?json=" + WebUtility.UrlEncode(this.CreateChannelSelectStringForURLParam(log));

            string response;
            using (Stream data = this.webClient.OpenRead(finalUrl))
            {
                StreamReader reader = new StreamReader(data);
                response = reader.ReadToEnd();
                reader.Close();
            }

            if (response != null && response.Length > 0)
            {
                List<List<String>> timeseries = JsonConvert.DeserializeObject<List<List<String>>>(response);
                result = this.CreateChannelValuesForResponse(timeseries);
            }
        } catch (System.Net.WebException webEx) { 
            
            log.Info($"Received web exception on m2mBackend REST call: {webEx.Message}");

            if ( (webEx.Status == WebExceptionStatus.ProtocolError)
                && (((HttpWebResponse)webEx.Response).StatusCode == HttpStatusCode.NotFound))
            {
                log.Info($"Tried to call m2mBackend REST API with non-existing site '{siteId}'");
            }
        }

        return result;
    }

    private List<ChannelValues> CreateChannelValuesForResponse(List<List<String>> timeseries)
    {
        List<ChannelValues> result = new List<ChannelValues>();

        if (timeseries != null && timeseries.Count > 0)
        {
            foreach (List<String> values in timeseries)
            {
                if (values != null && values.Count > 0 && values[0] != null)
                {
                    string timeStampString = values[0].PadRight(M2MBackendAPI.DATETIMEFORMAT.Length, '0');
                    ChannelValues newValue = new ChannelValues(
                        DateTime.ParseExact(timeStampString,
                        M2MBackendAPI.DATETIMEFORMAT, CultureInfo.InvariantCulture));

                    for(int i = 1; i < values.Count; i++)
                    {
                        if (!this.NotAvailableValue(values[i]))
                            newValue.AddChannelValue(this.channelList[i-1], 
                            int.Parse(values[i]));
                    }

                    result.Add(newValue);
                }
            }
        }

        return result;
    }

    private string CreateChannelSelectStringForURLParam(TraceWriter log)
    {
        // is {"select":["ch0","ch1"], "from" : "20170101", "until" : "*"}  

        String result = "{\"select\":[";
        if (this.channelList != null && this.channelList.Count > 0)
        {
            for (int i = 0; i < this.channelList.Count; i++) {
                result = result + "\"" + this.channelList[i] + "\"";
                if (i < (this.channelList.Count - 1))
                    result = result + ",";
            }
        }

        if (this.lastForward == null)
            result = result + "]}";
        else {
            result = result + "], \"from\" : \"" +
                this.CurrentBeginDateTime().ToString("o") + "\", " +
                "\"until\" : \"*\"}";
        }

        log.Info($"json Param for request: {result}");

        return result;
    }

    private DateTime CurrentBeginDateTime()
    {
        if (this.lastForward != null)
        {
            DateTime last = (DateTime)this.lastForward.LastValueFrom;
            return last.AddMilliseconds(100.0);
        } else
        {
            return new DateTime(2017, 1, 1);
        }
    }

    private bool NotAvailableValue(string channelValue)
    {
        bool result = true;

        if (channelValue != null)
            result = channelValue.Equals(M2MBackendAPI.NOT_A_NUMBER);

        return result;
    }

    public Timerange CreateCurrentTimerange(DateTime pollingTime, List<ChannelValues> values)
    {
        Timerange result = null;

        if (this.lastForward != null)
             result = new Timerange(this.CurrentBeginDateTime(), pollingTime);
        else
        {
            result = new Timerange(
                 DateTime.MinValue, pollingTime);
        }

        return result;
    }

    ~M2MBackendClient()
    {
        this.CloseWebClient();
        this.CloseDocDBClient();
    }

    public void Dispose()
    {
        this.CloseWebClient();
        this.CloseDocDBClient();
    }

    private void CloseWebClient()
    {
        if (this.webClient != null)
        {
            this.webClient.Dispose();
            this.webClient = null;
        }
    }
    private void CloseDocDBClient()
    {
        if (this.pollingDB != null)
        {
            this.pollingDB.Dispose();
            this.pollingDB = null;
        }
    }

}

public class M2MBackendAPI
{
    public static string NOT_A_NUMBER = "NAN";
    public static string DATETIMEFORMAT = "yyyyMMddHHmmssfff";
}