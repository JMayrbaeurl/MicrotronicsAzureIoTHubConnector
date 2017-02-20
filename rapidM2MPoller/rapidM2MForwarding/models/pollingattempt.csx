using Newtonsoft.Json;
public class PollingAttempt
{
    [JsonProperty(PropertyName = "id")]
    public string Id { get; set; }

    public String Customer_Id { get; set; }
    public String Site_Id { get; set; }

    public DateTime PollingTimestamp { get; set; }

    public DateTime? LastValueFrom { get; set;  }

    private List<ChannelValues> m2mData;

    public List<ChannelValues> M2MData {
        get { return this.m2mData;  }
        set
        {
            this.m2mData = value;
            this.LastValueFrom = null;
            this.UpdateLastValue();
        }
    }

    private void UpdateLastValue()
    {
        if (this.m2mData != null && this.m2mData.Count > 0)
        {
            DateTime? latest = null;
            foreach(ChannelValues values in this.m2mData)
            {
                if (latest == null)
                    latest = values.Timestamp;
                else
                {
                    if (values.Timestamp > latest)
                        latest = values.Timestamp;
                }
            }

            if (latest != null)
                this.LastValueFrom = latest;
        }
    }

    public MultipleDatapoints IoTHubMessage { get; set; }

    public override string ToString()
    {
        return JsonConvert.SerializeObject(this);
    }
}