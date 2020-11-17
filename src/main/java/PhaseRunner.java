import io.swagger.client.ApiException;
import io.swagger.client.ApiResponse;
import io.swagger.client.api.SkiersApi;
import io.swagger.client.model.LiftRide;
import io.swagger.client.model.SkierVertical;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import statistics.BulkRequestStatistics;
import statistics.SingleRequestStatistics;

/**
 * PhaseRunner uses the client SDK to call the server API in an automated way.
 * <p>
 * Source of truth for how phases are run is found here: https://gortonator.github.io/bsds-6650/assignments-2020/Assignment-1
 */
public class PhaseRunner implements Runnable {

    // Limited logging performed here due to high execution volume
    private static final Logger logger = LogManager.getLogger(PhaseRunner.class);


    private SkiersApi skiersApiInstance;
    private Arguments args;
    private CountDownLatch completionLatch;
    private CountDownLatch nextPhaseLatch;
    private BulkRequestStatistics stats;
    private ThreadLocalRandom rand;
    private SingleRequestStatistics[] singleRequestStatisticsArray;
    private int singleStatsCurrIndex;
    private int numPosts;
    private int numGets;
    private int skierIdLow;
    private int skierIdHigh;
    private int timeLow;
    private int timeHigh;

    /**
     * Basic constructor for a PhaseRunner.
     * <p>
     * To aid in readability, set skier and time ranges in helper methods, setSkierIdRange and
     * setTimeRange. These fields will be null if not set.
     *
     * @throws IllegalArgumentException if args is null or either numPosts or numGets is negative
     */
    public PhaseRunner(
            int numPosts,
            int numGets,
            Arguments args,
            CountDownLatch completionLatch,
            BulkRequestStatistics stats,
            CountDownLatch nextPhaseLatch)
            throws IllegalArgumentException {
        if (args == null || completionLatch == null || stats == null || numPosts < 0 || numGets < 0) {
            throw new IllegalArgumentException(
                    "invalid arguments - args cannot be null, posts and gets cannot be negative");
        }
        this.numPosts = numPosts;
        this.numGets = numGets;
        this.args = args;
        this.completionLatch = completionLatch;
        this.nextPhaseLatch = nextPhaseLatch;
        this.stats = stats;

        // Prevent null pointer errors if no next phase is given
        if (nextPhaseLatch == null) {
            this.nextPhaseLatch = new CountDownLatch(0);
        }

        // Set up api caller instance
        this.skiersApiInstance = new SkiersApi();
        this.skiersApiInstance.getApiClient().setBasePath(this.args.getHostAddress());

        // Thread-safe random number generator for generating API calls
        this.rand = ThreadLocalRandom.current();

        // Initialize array for all requests
        this.singleRequestStatisticsArray = new SingleRequestStatistics[this.numPosts + (this.numGets
                * 2)];  // 2x for Gets because there are two Get paths
        this.singleStatsCurrIndex = 0;
    }

    /**
     * Sets the skier ID range (inclusive) for this runner.
     *
     * @param low  low bound
     * @param high high bound
     * @throws IllegalArgumentException if invalid bounds are given
     */
    public void setSkierIdRange(int low, int high) throws IllegalArgumentException {
        // Basic validation (consider a separate class for arg validation)
        if (low < 0 || high < 0) {
            throw new IllegalArgumentException("bounds cannot be negative");
        }
        if (low > high) {
            throw new IllegalArgumentException("low bound cannot be greater than high bound");
        }
        this.skierIdLow = low;
        this.skierIdHigh = high;
    }

    /**
     * Sets the time range (inclusive) for this runner.
     *
     * @param low  low bound
     * @param high high bound
     * @throws IllegalArgumentException if invalid bounds are given
     */
    public void setTimeRange(int low, int high) {
        // Basic validation (consider a separate class for arg validation)
        if (low < 0 || high < 0) {
            throw new IllegalArgumentException("bounds cannot be negative");
        }
        if (low > high) {
            throw new IllegalArgumentException("low bound cannot be greater than high bound");
        }
        this.timeLow = low;
        this.timeHigh = high;
    }

    @Override
    public void run() {
        performPosts();
        performGets();
        stats.pushDataToWriter(singleRequestStatisticsArray);
        nextPhaseLatch.countDown();
        completionLatch.countDown();
    }

    /**
     * Runs the POST requests required against the server.
     */
    private void performPosts() {
        String reqType = "POST";
        String path = "/skiers/liftrides";

        // Set up reusable parts of a lift ride
        LiftRide liftRide = new LiftRide();
        liftRide.setResortID(args.getResort());
        liftRide.setDayID(
                String.valueOf(args.getSkiDay())
        );

        for (int i = 0; i < numPosts; i++) {
            // Set up random variables for skier, lift and time
            liftRide.setSkierID(nextSkierId());
            liftRide.setTime(nextTime());
            liftRide.setLiftID(nextLift());

            // Attempt request
            long reqStart = System.currentTimeMillis();
            try {
                // Get response info and time it. Write stats to array.
                ApiResponse<Void> resp = skiersApiInstance.writeNewLiftRideWithHttpInfo(liftRide);
                long reqEnd = System.currentTimeMillis();
                long latency = reqEnd - reqStart;
                appendStats(
                        new SingleRequestStatistics(reqType, path, reqStart, latency, resp.getStatusCode()));

                // Includes 4XX/5XX responses
            } catch (ApiException e) {
                // Get stats
                long reqEnd = System.currentTimeMillis();
                long latency = reqEnd - reqStart;
                appendStats(new SingleRequestStatistics(reqType, path, reqStart, latency, e.getCode()));
                stats.getTotalBadRequests().getAndIncrement();

                // Notify of error
                System.err.println("API error: " + e.getMessage());
                logger.error("API error: " + e.getMessage() + "\n"
                        + Arrays.toString(e.getStackTrace()));
            }
        }
    }

    /**
     * Runs the GET requests required against the server.
     */
    private void performGets() {
        performGetsVertByDayAndResort();
        performGetsVertByResort();
    }

    private void performGetsVertByDayAndResort() {
        String reqType = "GET";
        String path = "/skiers/{resortID}/days/{dayID}/skiers/{skierID}";

        for (int i = 0; i < numGets; i++) {
            long reqStart = System.currentTimeMillis();
            try {
                // Get response info and time it. Write stats to array.
                ApiResponse<SkierVertical> resp = skiersApiInstance.getSkierDayVerticalWithHttpInfo(
                        args.getResort(),
                        String.valueOf(args.getSkiDay()),
                        nextSkierId()
                );
                long reqEnd = System.currentTimeMillis();
                long latency = reqEnd - reqStart;
                appendStats(
                        new SingleRequestStatistics(reqType, path, reqStart, latency, resp.getStatusCode()));

                // Includes 4XX/5XX responses
            } catch (ApiException e) {
                // Record stats
                long reqEnd = System.currentTimeMillis();
                long latency = reqEnd - reqStart;
                appendStats(new SingleRequestStatistics(reqType, path, reqStart, latency, e.getCode()));
                stats.getTotalBadRequests().getAndIncrement();

                // Notify of error
                System.err.println("API error: " + e.getCode() + " " + e.getResponseBody());
                logger.error("API error: " + e.getCode() + " " + e.getResponseBody() + "\n"
                        + Arrays.toString(e.getStackTrace()));
            }
        }
    }

    private void performGetsVertByResort() {
        String reqType = "GET";
        String path = "/skiers/{skierID}/vertical";

        for (int i = 0; i < numGets; i++) {
            long reqStart = System.currentTimeMillis();
            try {
                // Get response info and time it. Write stats to array.
                ApiResponse<SkierVertical> resp = skiersApiInstance.getSkierResortTotalsWithHttpInfo(
                        nextSkierId(),
                        Collections.singletonList(args.getResort())
                );
                long reqEnd = System.currentTimeMillis();
                long latency = reqEnd - reqStart;
                appendStats(
                        new SingleRequestStatistics(reqType, path, reqStart, latency, resp.getStatusCode()));

                // Includes 4XX/5XX responses
            } catch (ApiException e) {
                // Record stats
                long reqEnd = System.currentTimeMillis();
                long latency = reqEnd - reqStart;
                appendStats(new SingleRequestStatistics(reqType, path, reqStart, latency, e.getCode()));
                stats.getTotalBadRequests().getAndIncrement();

                // Notify of error
                System.err.println("API error: " + e.getCode() + " " + e.getResponseBody());
                logger.error("API error: " + e.getCode() + " " + e.getResponseBody() + "\n"
                        + Arrays.toString(e.getStackTrace()));
            }
        }
    }

    /**
     * Adds the given stats to a storage array and ensures the correct index is used.
     *
     * @param stats the stats to store
     */
    private void appendStats(SingleRequestStatistics stats) {
        singleRequestStatisticsArray[singleStatsCurrIndex] = stats;
        singleStatsCurrIndex++;
    }

    private String nextSkierId() {
        return String.valueOf(rand.nextInt(skierIdLow, skierIdHigh + 1));
    }

    private String nextTime() {
        return String.valueOf(rand.nextInt(timeLow, timeHigh + 1));
    }

    private String nextLift() {
        // Values are increased by 1 in order to line up with 1-indexed lift IDs
        return String.valueOf(rand.nextInt(1, args.getNumSkiLifts() + 1));
    }
}
