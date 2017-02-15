public class MultipleDatapoints
{
    public String customer_id { get; set; }
    public String site_id { get; set; }
    public Timerange Timerange { get; set; }
    public List<TimeseriesEntry> Timeseries { get; set; }
}

public class Timerange
{
    public DateTime begin { get; set; }
    public DateTime end { get; set; }
}

public class TimeseriesEntry
{
    public DateTime Timestamp { get; set; }
    public Dictionary<string, int> Channels { get; set; }
}