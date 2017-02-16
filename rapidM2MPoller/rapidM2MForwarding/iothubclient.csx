#load ".\models\multipledatapoints.csx"

using System.Text;
using Microsoft.Azure.Devices;
using Microsoft.Azure.Devices.Client;

using Newtonsoft.Json;

public class IoTHubClient : IDisposable
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

    public void SendDataFrame(M2MBackendClient client, MultipleDatapoints datapoints, TraceWriter log)
    {
        if (datapoints != null && this.DoSend && this.iotHubClient != null)
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