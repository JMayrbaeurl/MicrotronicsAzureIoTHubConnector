#load ".\models\channelvalues.csx"
#load ".\models\multipledatapoints.csx"
#load ".\models\pollingattempt.csx"
#load ".\iothubclient.csx"

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
        List<ChannelValues> values = null;

        if (!client.IsEmulating) { 
            // Get latest not processed values from m2mBackend service
            values = client.GetNewTelemetryData(myTimer, log);

            if (values != null)
                log.Info($"Calling m2mBackend returned {values.Count} channel values");
        } else
        {
            ChannelValues aValue = new ChannelValues(DateTime.Now);
            aValue.AddChannelValue(client.ChannelNames[0], 
                (int)(DateTime.Now - new DateTime(2017,1,1)).TotalSeconds);
            values = new List<ChannelValues>();
            values.Add(aValue);

            log.Info($"Generated random values for channels");
        }

        // First log polling attempt to DocDB
        PollingAttempt polling = CreatePollingAttemptForChannelValues(values);
        if (polling != null)
            client.WritePollingAttemptToDB(polling, log).Wait();

        // If we have got data forward them to IoT Hub
        if (values != null && values.Count > 0)
        {
            string connString = System.Configuration.ConfigurationManager.AppSettings.Get("IoTHubConnection");
            using (IoTHubClient iothubclient = IoTHubClient.CreateNewInstance(connString))
            {
                MultipleDatapoints datapoints = CreateDatapointsFromChannelValues(client, values);
                if (datapoints != null)
                {
                    iothubclient.SendDataFrame(client, datapoints);
                    log.Info($"Successfully forwarded data points to IoT Hub");
                }
            }
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
    result.FindLastPollingEntry(log);

    result.IsEmulating = Boolean.TrueString.ToLower().Equals(emulModeString.ToLower());

    return result;
}

private static PollingAttempt CreatePollingAttemptForChannelValues(List<ChannelValues> values)
{
    PollingAttempt polling = new PollingAttempt();
    polling.PollingTimestamp = DateTime.Now;
    TimeSpan timeSpanSince2017 = polling.PollingTimestamp - new DateTime(2017, 1, 1);
    polling.Id = timeSpanSince2017.TotalMilliseconds.ToString();
    polling.M2MData = values;

    return polling;
}

private static MultipleDatapoints CreateDatapointsFromChannelValues(M2MBackendClient m2mBackendClient, List<ChannelValues> values)
{
    MultipleDatapoints datapoints = new MultipleDatapoints();

    datapoints.customer_id = m2mBackendClient.Customer_Id;
    datapoints.site_id = m2mBackendClient.Site_Id;

    datapoints.Timerange = m2mBackendClient.CreateCurrentTimerange(values);

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

    private string site_id = null;
    public string Site_Id { get { return this.site_id; } }

    private List<String> channelList = new List<String>() { "ch0", "ch3", "ch1" };
    public List<String> ChannelNames { get { return this.channelList;  } }

    private string databaseName = "M2MBackendForwarderData";
    private string collectionName = "pollingAttempts";

    private WebClient webClient = null;
    private DocumentClient docClient = null;

    private PollingAttempt lastAttempt = null;

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
        this.docClient = new DocumentClient(new Uri(dbUri), dbKey);
    }

    public void SetIdentification(string custId, string siteId)
    {
        this.customer_id = custId;
        this.site_id = siteId;
    }

    public async Task CreateDatabaseIfNotExists(TraceWriter log)
    {
        if (this.docClient != null)
        {
            // Check to verify a database with the id=FamilyDB does not exist
            try
            {
                await this.docClient.ReadDatabaseAsync(UriFactory.CreateDatabaseUri(this.databaseName));
                log.Info($"Found {this.databaseName}");
            }
            catch (DocumentClientException de)
            {
                // If the database does not exist, create a new database
                if (de.StatusCode == HttpStatusCode.NotFound)
                {
                    await this.docClient.CreateDatabaseAsync(new Database { Id = this.databaseName });
                    log.Info($"Created {this.databaseName}");
                }
                else
                {
                    throw;
                }
            }
        }
    }

    public async Task CreatePollingHistoryCollectionIfNotExists(TraceWriter log)
    {
        try
        {
            await this.docClient.ReadDocumentCollectionAsync(
                UriFactory.CreateDocumentCollectionUri(this.databaseName, this.collectionName));
            log.Info($"Found {this.collectionName} collection");
        }
        catch (DocumentClientException de)
        {
            // If the document collection does not exist, create a new collection
            if (de.StatusCode == HttpStatusCode.NotFound)
            {
                DocumentCollection collectionInfo = new DocumentCollection();
                collectionInfo.Id = this.collectionName;

                // Configure collections for maximum query flexibility including string range queries.
                collectionInfo.IndexingPolicy = new IndexingPolicy(new RangeIndex(DataType.String) { Precision = -1 });

                // Here we create a collection with 400 RU/s.
                await this.docClient.CreateDocumentCollectionAsync(
                    UriFactory.CreateDatabaseUri(this.databaseName),
                    collectionInfo,
                    new RequestOptions { OfferThroughput = 400 });

                log.Info($"Created {this.collectionName} collection");
            }
            else
            {
                throw;
            }
        }
    }

    public void FindLastPollingEntry(TraceWriter log)
    {
        if (this.docClient != null) {
            IQueryable<PollingAttempt> queryResult = 
                this.docClient.CreateDocumentQuery<PollingAttempt>(
                    UriFactory.CreateDocumentCollectionUri(this.databaseName, this.collectionName),
                    "SELECT TOP 1 * FROM pollingAttempts p ORDER BY p.LastValueFrom DESC");

            if (queryResult != null && queryResult.AsEnumerable().Count() > 0)
            {
                this.lastAttempt = queryResult.AsEnumerable().First();
            }  
        }
    }

    public async Task WritePollingAttemptToDB(PollingAttempt polling, TraceWriter log)
    {
        try
        {
            await this.docClient.ReadDocumentAsync(UriFactory.CreateDocumentUri(this.databaseName, 
                this.collectionName, polling.Id));
            log.Info($"Found {polling.Id}");
        }
        catch (DocumentClientException de)
        {
            if (de.StatusCode == HttpStatusCode.NotFound)
            {
                await this.docClient.CreateDocumentAsync(UriFactory.CreateDocumentCollectionUri(
                    this.databaseName, this.collectionName), polling);
                log.Info($"Created Polling entry {polling.Id}");
            }
            else
            {
                throw;
            }
        }
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

    public List<ChannelValues> GetNewTelemetryData(TimerInfo myTimer, TraceWriter log)
    {
        List<ChannelValues> result = new List<ChannelValues>();

        string finalUrl = this.url +
            $"customers/{this.customer_id}/sites/{this.site_id}/histdata0" +
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

        if (this.lastAttempt == null)
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
        if (this.lastAttempt != null)
        {
            DateTime last = (DateTime)this.lastAttempt.LastValueFrom;
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

    public Timerange CreateCurrentTimerange(List<ChannelValues> values)
    {
        Timerange result = new Timerange(
            this.CurrentBeginDateTime(), this.lastAttempt.PollingTimestamp);

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
        if (this.docClient != null)
        {
            this.docClient.Dispose();
            this.docClient = null;
        }
    }

}

public class M2MBackendAPI
{
    public static string NOT_A_NUMBER = "NAN";
    public static string DATETIMEFORMAT = "yyyyMMddHHmmssfff";
}