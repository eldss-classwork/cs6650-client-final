package statistics;

/**
 * Holds and provides access to statistics from a single request.
 */
public class SingleRequestStatistics {

    private String requestType;
    private String path;
    private long startTime;
    private long latency;
    private int responseCode;

    /**
     * Constructor for statistics.SingleRequestStatistics.
     *
     * @param requestType  The request type (i.e. "GET" or "POST")
     * @param startTime    The unix time at the start of the request
     * @param latency      The latency of the request (e.g. the round trip time)
     * @param responseCode The response code returned from the server
     */
    public SingleRequestStatistics(String requestType, String path, long startTime, long latency,
                                   int responseCode) {
        this.requestType = requestType;
        this.path = path;
        this.startTime = startTime;
        this.latency = latency;
        this.responseCode = responseCode;
    }

    public String getRequestType() {
        return requestType;
    }

    public String getPath() {
        return path;
    }

    public long getStartTime() {
        return startTime;
    }

    public long getLatency() {
        return latency;
    }

    public int getResponseCode() {
        return responseCode;
    }

}
