package statistics;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.BlockingQueue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Writes request statistics to a CSV file during execution of the requests. Managed by the bulk
 * stats object which passes chunks of data from a queue.
 */
public class CsvStatsWriter {

    private static final Logger logger = LogManager.getLogger(CsvStatsWriter.class);

    private String filePath;
    private PrintWriter pw;
    private BlockingQueue<SingleRequestStatistics[]> writeQueue;

    public CsvStatsWriter(String csvPathStr,
                          BlockingQueue<SingleRequestStatistics[]> writeQueue) {
        this.filePath = csvPathStr;
        this.writeQueue = writeQueue;
    }

    /**
     * Starts a loop that writes data to a csv file in a new thread and returns the thread handle.
     * Exits the program if there is a problem with the thread.
     *
     * @return the thread handle
     */
    public Thread startWriteLoop() {
        Runnable loopWork = () -> {
            try {
                writeLoop();
            } catch (InterruptedException e) {
                fatal("csv writing thread was interrupted. Stopping...");
            }
        };
        Thread loop = new Thread(loopWork);
        loop.start();
        return loop;
    }

    /**
     * The actual loop writing work. When the final values are written, closes the print writer.
     *
     * @throws InterruptedException if there is a problem with the blocking queue
     */
    private void writeLoop() throws InterruptedException {
        SingleRequestStatistics[] threadData = writeQueue.take();

        // Empty array will signal that there is not more data
        while (threadData.length != 0) {
            // Print each record to the csv
            for (SingleRequestStatistics stats : threadData) {
                String line = buildCsvLine(stats);
                pw.println(line);
            }

            // Get next thread's data
            threadData = writeQueue.take();
        }

        pw.close();
    }

    /**
     * Erases any old file, creates a new one and writes the correct CSV headers. Keeps the file open
     * and ready to write to.
     */
    public void initCsvFile() {
        File csvFile = new File(filePath);

        // Ensure any old file is overwritten
        if (csvFile.exists()) {
            // First try deleting the old file, then try creating a new one
            try {
                csvFile.delete();
                csvFile.createNewFile();
            } catch (IOException e) {
                String msg = "Problem overwriting existing CSV file";
                fatal(msg);
            }
        }

        try {
            pw = new PrintWriter(csvFile);
        } catch (FileNotFoundException e) {
            String msg = "Problem creating new file";
            fatal(msg);
        }

        // Print headers but keep writer open to receive more data
        String headers = "RequestType,Path,StartTimestamp(ms),Latency(ms),ResponseCode";
        pw.println(headers);
    }

    /**
     * Writes a csv file containing data for a histogram of the number of requests started during each
     * minute of program execution.
     *
     * @param outFilePath the output file path, if exists, will be overwritten
     * @param data        the data to write, with the indices being the minute buckets
     */
    public void writeRequestStartData(String outFilePath, long[] data) {
        File csvFile = new File(outFilePath);

        // Ensure any old file is overwritten
        if (csvFile.exists()) {
            // First try deleting the old file, then try creating a new one
            try {
                csvFile.delete();
                csvFile.createNewFile();
            } catch (IOException e) {
                String msg = "Problem overwriting existing CSV file";
                fatal(msg);
            }
        }

        // Write new data
        try (PrintWriter writer = new PrintWriter(csvFile)) {
            // Print headers but keep writer open to receive more data
            String headers = "Minute,Num Requests Started";
            writer.println(headers);

            // Print data
            for (int i = 0; i < data.length; i++) {
                String line = i + "," + data[i];
                writer.println(line);
            }

        } catch (Exception e) {
            String msg = "Problem writing new histogram data file";
            System.err.println(msg + " - " + e.getMessage());
        }

    }

    /**
     * Creates a one line string in CSV format from a statistics.SingleRequestStatistics object.
     *
     * @param singleStats the stats to make a string
     * @return A string with the stats in CSV format
     */
    private String buildCsvLine(SingleRequestStatistics singleStats) {
        String type = singleStats.getRequestType();
        String path = singleStats.getPath();
        String start = String.valueOf(singleStats.getStartTime());
        String latency = String.valueOf(singleStats.getLatency());
        String code = String.valueOf(singleStats.getResponseCode());

        String[] data = new String[]{type, path, start, latency, code};
        return String.join(",", data);
    }

    /**
     * Stops the program with an error message upon encountering an error that prevents the client
     * from working correctly.
     *
     * @param msg a message to log
     */
    private void fatal(String msg) {
        logger.error(msg);
        System.err.println(msg);
        System.exit(1);
    }
}
