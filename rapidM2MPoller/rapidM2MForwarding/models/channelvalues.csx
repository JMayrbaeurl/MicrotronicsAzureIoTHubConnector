public class ChannelValues
{
    public DateTime Timestamp { get; set; }
    public Dictionary<string, int> Values { get; set; }

    public ChannelValues(DateTime forTime)
    {
        this.Timestamp = forTime;
        this.Values = new Dictionary<string, int>();
    }

    public void AddChannelValue(string channelDesc, int channelValue)
    {
        this.Values.Add(channelDesc, channelValue);
    }
}