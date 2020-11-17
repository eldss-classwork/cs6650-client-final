package statistics;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

/**
 * Provides methods to calculate statistics from the CSV request file created during client
 * execution.
 */
public class CsvStatsReader {

    // For CSV splitting in calculation methods
    private final int csvColIndexMethod = 0;
    private final int csvColIndexPath = 1;
    private final int csvColIndexTimestamp = 2;
    private final int csvColIndexLatency = 3;
    private final int csvColIndexCode = 4;
    private final String SEP = ",";

    private Path filePath;

    public CsvStatsReader(String csvPathStr) {
        this.filePath = Paths.get(csvPathStr);
    }

    /**
     * Calculates the mean latency for each request type from a CSV.
     */
    public Map<String, Double> calculateMeanLatencies() throws IOException, NumberFormatException {
        // Stores "method path" -> [sum, count]
        Map<String, Long[]> tempCounter = new HashMap<>();
        Map<String, Double> avgByPath = new HashMap<>();

        BufferedReader reader = Files.newBufferedReader(filePath);
        String line = reader.readLine(); // Ignore column headers
        line = reader.readLine();
        while (line != null) {
            // Get vals from line
            String[] cols = line.split(SEP);
            String key = makeKey(cols);
            long nextLatency = Long.parseLong(getLatency(cols));

            // Update map
            Long[] currNums = tempCounter.getOrDefault(key, new Long[]{(long) 0, (long) 0});
            currNums[0] += nextLatency;
            currNums[1] += (long) 1;
            tempCounter.put(key, currNums);

            // Get next line
            line = reader.readLine();
        }

        // Calculate the avg for each path
        for (String key : tempCounter.keySet()) {
            Long[] nums = tempCounter.get(key);

            // Ignore anything that got in with a count of zero
            // and avoid divide by zero
            if (nums[1].equals((long) 0)) {
                continue;
            }

            // Store the value
            Double avg = (double) nums[0] / nums[1];
            avgByPath.put(key, avg);
        }

        return avgByPath;
    }

    /**
     * Calculates the maximum latency for each request type from a CSV.
     */
    public Map<String, Integer> calculateMaxLatencies() throws IOException, NumberFormatException {
        // Stores "method path" -> max
        Map<String, Integer> maxByPath = new HashMap<>();

        BufferedReader reader = Files.newBufferedReader(filePath);
        String line = reader.readLine(); // Ignore column headers
        line = reader.readLine();
        while (line != null) {
            // Parse data
            String[] cols = line.split(SEP);
            String key = makeKey(cols);
            int latency = Integer.parseInt(getLatency(cols));

            // Check max and update
            Integer max = maxByPath.getOrDefault(key, 0);
            if (latency > max) {
                maxByPath.put(key, latency);
            }

            line = reader.readLine();
        }

        return maxByPath;
    }

    /**
     * Calculates median latencies for each request path from a CSV.
     */
    public Map<String, Integer> calculateMedianLatencies(Map<String, Integer> pathsToMax)
            throws IOException, NumberFormatException {
        // Used to count the number of times a specific latency occurs in a path
        Map<String, Integer[]> pathsToCountingArray = buildCountingArrayMap(pathsToMax);

        // Get the median and add it to the result map
        Map<String, Integer> pathsToMedian = new HashMap<>();
        for (String key : pathsToCountingArray.keySet()) {
            Integer[] counter = pathsToCountingArray.get(key);
            int median = getMedianFromCountingArray(counter);
            pathsToMedian.put(key, median);
        }

        return pathsToMedian;
    }

    /**
     * Calculates median latencies for each request path from a CSV.
     */
    public Map<String, Integer> calculateP99Latencies(Map<String, Integer> pathsToMax)
            throws IOException, NumberFormatException {
        // Used to count the number of times a specific latency occurs in a path
        Map<String, Integer[]> pathsToCountingArray = buildCountingArrayMap(pathsToMax);

        // Get the 99th percentile and add it to the result map
        Map<String, Integer> pathsToP99 = new HashMap<>();
        for (String key : pathsToCountingArray.keySet()) {
            Integer[] counter = pathsToCountingArray.get(key);
            int p99 = getP99FromCountingArray(counter);
            pathsToP99.put(key, p99);
        }

        return pathsToP99;
    }

    /**
     * Calculates the number of requests started during each second of program operation.
     */
    public long[] calculateNumRequestsByMin(long startTimestamp, long endTimestamp)
            throws IOException, NumberFormatException {
        double totalSecs = milliSecsToSecs(endTimestamp - startTimestamp);
        int length = (int) Math.ceil(totalSecs);
        long[] result = new long[length];
        int timestampIndex = 2;

        // Start reading file
        BufferedReader reader = Files.newBufferedReader(filePath);
        String line = reader.readLine(); // Ignore column headers
        line = reader.readLine();
        while (line != null) {
            // Parse data
            String[] cols = line.split(SEP);
            long timeOfRequest = Long.parseLong(cols[timestampIndex]);

            // Increment count on appropriate second
            int secondBucket = (int) Math.floor(
                    milliSecsToSecs(timeOfRequest - startTimestamp)
            );
            result[secondBucket]++;

            line = reader.readLine();
        }

        return result;
    }

    /**
     * Builds a latency counting array from a CSV file, given the max latencies in the file.
     *
     * @param pathsToMax a path to max latency map
     * @return the path to counting array produced
     * @throws IOException           if there is a problem reading from the file
     * @throws NumberFormatException if there is a problem parsing the latency value
     */
    private Map<String, Integer[]> buildCountingArrayMap(Map<String, Integer> pathsToMax)
            throws IOException, NumberFormatException {
        Map<String, Integer[]> pathsToCountingArray = new HashMap<>();

        // Initialize above map
        // Keys are the same, taken from CSV (assumed)
        for (String key : pathsToMax.keySet()) {
            int maxVal = pathsToMax.get(key);
            pathsToCountingArray.put(key, new Integer[maxVal + 1]);  // Add 1 to account for latency=0
        }

        // Start the count
        BufferedReader reader = Files.newBufferedReader(filePath);
        String line = reader.readLine(); // Ignore column headers
        line = reader.readLine();
        while (line != null) {
            // Parse data
            String[] cols = line.split(SEP);
            String key = makeKey(cols);
            int latency = Integer.parseInt(getLatency(cols));

            // Update counting array
            Integer[] counter = pathsToCountingArray.get(key);
            if (counter[latency] == null) {
                counter[latency] = 1;
            } else {
                counter[latency]++;
            }

            line = reader.readLine();
        }

        return pathsToCountingArray;
    }

    /**
     * Builds a key out the request method and path in the form of "method path" as a String.
     *
     * @param cols a line of CSV data, split into an array
     * @return the String key
     */
    private String makeKey(String[] cols) {
        return cols[csvColIndexMethod] + " " + cols[csvColIndexPath];
    }

    /**
     * Convenience method for getting the latency column
     */
    private String getLatency(String[] cols) {
        return cols[csvColIndexLatency];
    }

    /**
     * Helper for finding the median of an array efficiently.
     *
     * @param arr an array in which the index is the latency and the values are the counts of requests
     *            that had that latency
     * @return the median latency
     */
    private int getMedianFromCountingArray(Integer[] arr) {
        long numRequests = getSum(arr);
        int middleRequest = (int) Math.round(numRequests / 2.0);  // approximate in some cases
        int currTotal = 0;
        for (int i = 0; i < arr.length; i++) {
            // Find the middle request bucket: its index is the median
            if (arr[i] != null) {
                currTotal += arr[i];
            }
            if (currTotal >= middleRequest) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Helper for finding the 99th percentile of an array efficiently.
     *
     * @param arr an array in which the index is the latency and the values are the counts of requests
     *            that had that latency
     * @return the median latency
     */
    private int getP99FromCountingArray(Integer[] arr) {
        long numRequests = getSum(arr);
        long p99Request = Math.round(numRequests * 0.99);  // approximate for decimal values
        long currTotal = numRequests;
        for (int i = arr.length - 1; i >= 0; i--) {
            // Find the bucket of the p99 request: its index is value we want
            if (arr[i] != null) {
                currTotal -= arr[i];
            }
            if (currTotal <= p99Request) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Simple sum calculator.
     *
     * @param arr an array of ints
     * @return the sum of the ints
     */
    private long getSum(Integer[] arr) {
        if (arr == null) {
            return -1;
        }
        long sum = 0;
        for (Integer num : arr) {
            if (num != null) {
                sum += num;
            }
        }
        return sum;
    }

    /**
     * Converts milliseconds to seconds
     *
     * @param milliseconds a value in milliseconds
     * @return the value of milliseconds as seconds
     */
    private double milliSecsToSecs(long milliseconds) {
        int millisecsPerSec = 1000;
        return (double) milliseconds / millisecsPerSec;
    }
}
