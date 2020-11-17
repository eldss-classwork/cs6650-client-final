import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

/**
 * Basic class to store command line arguments. Once created, an instance
 * is immutable.
 */
public class Arguments {

    final private int maxThreads;
    final private int numSkiers;
    final private int numSkiLifts;
    final private int skiDay;
    final private int dayLengthMinutes = 420; // stored here because potentially customizable in future
    final private String resort;
    final private String hostAddress;
    final private String csvFilename;

    /**
     * Private constructor for use with factory methods.
     */
    private Arguments(int maxThreads, int numSkiers, int numSkiLifts, int skiDay,
                      String resort, String hostAddress, String csvFilename) {
        this.maxThreads = maxThreads;
        this.numSkiers = numSkiers;
        this.numSkiLifts = numSkiLifts;
        this.skiDay = skiDay;
        this.resort = resort;
        this.hostAddress = hostAddress;
        this.csvFilename = csvFilename;
    }

    /**
     * Creates an Arguments instance from a properties file.
     * Available properties:
     *   - maxThreads (min:4)
     *   - numSkiers (default: 50000, min: 1, max: 50000)
     *   - numSkiLifts (default: 40, min: 5, max:60)
     *   - skiDay (default: 1, min: 1, max: 366)
     *   - resort: String
     *   - hostAddress: String
     *   - csvFilename: String
     * maxThreads, resortId and hostAddr are required.
     * @param fileName Path to the properties file
     * @return an Arguments instance with the specified properties
     * @throws IOException if there's a problem reading the file
     * @throws IllegalArgumentException if any properties are invalid
     */
    public static Arguments fromPropertiesFile(String fileName)
            throws IOException, IllegalArgumentException {
        FileInputStream fis = null;
        Properties props = null;

        // Read the file and extract contents
        try {
            fis = new FileInputStream(fileName);
            props = new Properties();
            props.load(fis);
        } catch(FileNotFoundException fnfe) {
            System.err.println("Could not find properties file: " + fnfe.getMessage());
        } catch(IOException ioe) {
            System.err.println("Problem reading properties file: " + ioe.getMessage());
        } finally {
            if (fis != null) {
                fis.close();
            }
        }
        return fromProperties(props);
    }

    /**
     * Helper method to validate property file values and create an Arguments instance.
     * @param props a Properties object obtained by parsing a properties file
     * @return an Arguments instance with the given properties
     * @throws IllegalArgumentException if given invalid properties
     */
    private static Arguments fromProperties(Properties props) throws IllegalArgumentException {
        if (props == null) {
            throw new IllegalArgumentException("no arguments given");
        }

        // Final value vars
        int maxThreads, numSkiers, numSkiLifts, skiDay;
        String resort, hostAddress, csvFilename;

        // Defaults and property names
        String skiersDefault = "50000";
        String liftsDefault = "40";
        String dayDefault = "1";
        String resortName = "resort";
        String hostAddressName = "hostAddress";
        String csvFilenameName = "csvFilename";
        String threadsName = "maxThreads";
        String skiersName = "numSkiers";
        String liftsName = "numSkiLifts";
        String dayName = "skiDay";

        // Check required fields are given
        resort = props.getProperty(resortName);
        hostAddress = props.getProperty(hostAddressName);
        String maxThreadsRaw = props.getProperty(threadsName);
        if (resort == null || hostAddress == null || maxThreadsRaw == null) {
            throw new IllegalArgumentException("properties file missing required fields");
        }

        // Get non-required csv filename
        csvFilename = props.getProperty(csvFilenameName);

        // Get and convert numerical fields
        try {
            maxThreads = Integer.parseInt(maxThreadsRaw);
            numSkiers = Integer.parseInt(
                    props.getProperty(skiersName, skiersDefault)
            );
            numSkiLifts = Integer.parseInt(
                    props.getProperty(liftsName, liftsDefault)
            );
            skiDay = Integer.parseInt(
                    props.getProperty(dayName, dayDefault)
            );
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "could not parse properties file - malformed numerical data");
        }

        // Validate numerical fields
        // Check separately for better error messages
        boolean threadsCondition = (maxThreads >= 4);
        // Upper limit can only change if more skiers are added to the database
        boolean skiersCondition = (numSkiers > 0 && numSkiers <= 50000);
        boolean liftsCondition = (numSkiLifts >= 5 && numSkiLifts <= 60);
        boolean dayCondition = (skiDay >= 1 && skiDay <= 366);
        if (!threadsCondition) {
            throw new IllegalArgumentException("maxThreads must be greater than 4");
        }
        if (!skiersCondition) {
            throw new IllegalArgumentException("numSkiers cannot be negative");
        }
        if (!liftsCondition) {
            throw new IllegalArgumentException("numSkiLifts must be between 5 and 60, inclusive");
        }
        if (!dayCondition) {
            throw new IllegalArgumentException("skiDay must be between 1 and 366, inclusive");
        }

        // Finally we can create an Arguments instance
        return new Arguments(maxThreads, numSkiers, numSkiLifts, skiDay, resort, hostAddress, csvFilename);
    }

    public int getMaxThreads() {
        return maxThreads;
    }

    public int getNumSkiers() {
        return numSkiers;
    }

    public int getNumSkiLifts() {
        return numSkiLifts;
    }

    public int getSkiDay() {
        return skiDay;
    }

    public int getDayLengthMinutes() {
        return dayLengthMinutes;
    }

    public String getResort() {
        return resort;
    }

    public String getHostAddress() {
        return hostAddress;
    }

    public String getCsvFilename() {
        return this.csvFilename;
    }

    @Override
    public String toString() {
        return "Arguments{" +
                "maxThreads=" + maxThreads +
                ", numSkiers=" + numSkiers +
                ", numSkiLifts=" + numSkiLifts +
                ", skiDay=" + skiDay +
                ", dayLengthMinutes=" + dayLengthMinutes +
                ", resort='" + resort + '\'' +
                ", hostAddress='" + hostAddress + '\'' +
                '}';
    }
}
