#load ".\models\multipledatapoints.csx"

using System.Text;
using System.Net;
using Microsoft.Azure.Devices;
using Microsoft.Azure.Devices.Client;

using Newtonsoft.Json;

public interface IoTHubSender : IDisposable
{
    bool DoesSend();
    void SendDataFrame(M2MBackendClient client, MultipleDatapoints datapoints, TraceWriter log);
}

public class IoTHubSenderFactory
{
    public string ConnectionString { get; set; }

    public string IoTHubURL { get; set; }
    public string IotHubDeviceId { get; set; }
    public string IotHubSASignature { get; set; }

    public IoTHubSender CreateNewInstance(TraceWriter log)
    {
        if (this.ConnectionString != null)
            return IoTHubClient.CreateNewInstance(this.ConnectionString, log);
        else
        {
            return IoTHubWebClient.CreateNewInstance(this.IoTHubURL,
                this.IotHubDeviceId, this.IotHubSASignature, log);
        }
    }
}

public class IoTHubClient : IoTHubSender
{
    private DeviceClient iotHubClient = null;

    public bool DoSend { get; set; }

    public static IoTHubClient CreateNewInstance(string iotHubConnString, TraceWriter log)
    {
        IoTHubClient result = new IoTHubClient();
        result.DoSend = true;

        result.iotHubClient = DeviceClient.CreateFromConnectionString(iotHubConnString,
            Microsoft.Azure.Devices.Client.TransportType.Mqtt);

        log.Info("Created device client for Azure IoT Hub");

        return result;
    }

    public bool DoesSend()
    {
        return this.DoSend;
    }
     
    public void SendDataFrame(M2MBackendClient client, MultipleDatapoints datapoints, TraceWriter log)
    {
        if (datapoints != null && this.DoesSend() && this.iotHubClient != null)
        {
            var message = new Message(Encoding.ASCII.GetBytes(JsonConvert.SerializeObject(datapoints)));

            message.Properties.Add("Customer_Id", client.Customer_Id);
            message.Properties.Add("Site_Id", client.Site_Id);

            log.Info("Now sending device data from m2m to IoT Hub");

            Task task = iotHubClient.SendEventAsync(message);
            task.Wait();
        }
    }

    public void Dispose()
    {
        this.CloseIoTHubClient();
    }

    ~IoTHubClient()
    {
        this.CloseIoTHubClient();
    }

    private void CloseIoTHubClient()
    {
        if (this.iotHubClient != null)
        {
            this.iotHubClient.Dispose();
            this.iotHubClient = null;
        }
    }
}
public class IoTHubWebClient : IoTHubSender
{
    private WebClient iotHubWebClient;

    public string IoTHubURL { get; set; }
    public string IotHubDeviceId { get; set; }
    public string IotHubSASignature { get; set; }

    public IoTHubWebClient ()
    {
        WebClient webClient = new WebClient();
        webClient.Headers.Add("user-agent",
               "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.2; .NET CLR 1.0.3705;)");

        this.iotHubWebClient = webClient;
    }

    public static IoTHubWebClient CreateNewInstance(string url, string deviceId, string sas, TraceWriter log)
    {
        IoTHubWebClient result = new IoTHubWebClient()
        {
            IoTHubURL = url,
            IotHubDeviceId = deviceId,
            IotHubSASignature = sas
        };

        log.Info("Created device client for Azure IoT Hub");

        return result;
    }

    public bool DoesSend() { return true; }

    public void SendDataFrame(M2MBackendClient client, MultipleDatapoints datapoints, TraceWriter log)
    {
        if (datapoints != null && this.DoesSend() && this.iotHubWebClient != null)
        {
            log.Info("Now sending device data from m2m to IoT Hub");

            this.iotHubWebClient.Headers.Add("Authorization", this.IotHubSASignature);

            string finalUrl = $"https://{this.IoTHubURL}/devices/{this.IotHubDeviceId}/messages/events?api-version=2016-11-14";
            using (Stream data = this.iotHubWebClient.OpenWrite(finalUrl, "POST"))
            {
                string postData = JsonConvert.SerializeObject(datapoints);
                byte[] postArray = Encoding.ASCII.GetBytes(postData);

                try
                {
                    data.Write(postArray, 0, postArray.Length);
                } catch (System.Net.WebException ex)
                {
                    log.Info($"IoT Hub returned error: {ex.Message}");
                }
            }
        }
    }

    public void Dispose()
    {
        this.CloseIoTHubWebClient();
    }

    ~IoTHubWebClient()
    {
        this.CloseIoTHubWebClient();
    }

    private void CloseIoTHubWebClient()
    {
        if (this.iotHubWebClient != null)
        {
            this.iotHubWebClient.Dispose();
            this.iotHubWebClient = null;
        }
    }
}