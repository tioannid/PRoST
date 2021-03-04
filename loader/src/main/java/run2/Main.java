package run2;

import loader2.*;
import org.apache.commons.cli.*;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.sql.SparkSession;

import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import loader2.configuration.TripleTableSchema;

/**
 * The Main class parses the CLI arguments and calls the executor.
 * <p>
 * Options: -h, --help prints the usage help message. -i, --input <file> HDFS
 * input path of the RDF graph. -o, --output <DBname> output database name. -s,
 * compute statistics
 *
 * @author Matteo Cossu
 * @author Victor Anthony Arrascue Ayala
 */
public class Main {

    public static String appName = "PRoST Loader-2";
    private static String input_location;
    private static String outputDB;
    private static boolean dropDB;
    private static String lpStrategies;
    private static String loj4jFileName = "log4j.properties";
    private static final Logger logger = Logger.getLogger(appName);
    private static boolean useStatistics = false;
    private static boolean dropDuplicates = true;
    private static boolean generateTT = false;
    private static boolean generateWPT = false;
    private static boolean generateVP = false;
    private static boolean generateExtVP = false;
    private static boolean generateDEC = false;
    private static boolean generatePEC = false;

    // options for physical partitioning
    private static boolean ttPartitionedByPred = false;
    private static boolean ttPartitionedBySub = false;
    private static String statFile;
    private static String dictionaryFile;
    private static String namespacePrefixFile = "";
    private static String asWKTFile = "";
    private static boolean nsPrefixDictEncode = false; // do not encode with Namespace Prefix dictionary
    private static double thresholdExtVP = 0.25;
    private static String tripleTable = "triples";

    private static boolean flagDBExists = false; // DB exists? Assume not!
    private static boolean flagCreateDB = !flagDBExists;  // Create DB!
    private static TripleTableSchema tttschema = new TripleTableSchema();
    private static TripleTableSchema gttschema = new TripleTableSchema();
    private static boolean useHiveQL_TableCreation = false; // use Spark SQL
    private static String hiveTableFormat = "TextFile";   //  TextFile, SequenceFile, RCfile, ORC, and Parquet

    public static void main(final String[] args) throws Exception {
        final InputStream inStream = Main.class.getClassLoader().getResourceAsStream(loj4jFileName);
        final Properties props = new Properties();
        props.load(inStream);
        PropertyConfigurator.configure(props);
        long start = System.currentTimeMillis();
        /*
		 * Manage the CLI options
         */
        final CommandLineParser parser = new PosixParser();
        final Options options = new Options();

        final Option inputOpt = new Option("i", "input", true, "HDFS input path of the RDF graph.");
        inputOpt.setRequired(true);
        options.addOption(inputOpt);

        final Option outputOpt = new Option("o", "output", true, "Output database name.");
        outputOpt.setRequired(true);
        options.addOption(outputOpt);

        // tioa
        final Option dropDbOpt = new Option("drdb", "dropdb", true, "Drop database.");
        dropDbOpt.setRequired(false);
        options.addOption(dropDbOpt);

        final Option statFileOpt = new Option("sf", "statisticsfile", true, "Statistics Filename.");
        statFileOpt.setRequired(false);
        options.addOption(statFileOpt);

        final Option dictFileOpt = new Option("df", "dictionaryfile", true, "Dictionary Filename.");
        dictFileOpt.setRequired(false); // required for VP but not for TT
        options.addOption(dictFileOpt);

        // tioa
        // Namespace Prefix file in JSON format
        final Option namespacePrefixFileOpt = new Option("nsprf", "namespaceprefixfile", true, "Namespace Prefix Filename.");
        namespacePrefixFileOpt.setRequired(false);
        options.addOption(namespacePrefixFileOpt);

        // tioa
        // Namespace Prefix file in JSON format
        final Option asWKTFileOpt = new Option("aswkt", "propAsWKT", true, "#asWKT properties file.");
        asWKTFileOpt.setRequired(false);
        options.addOption(asWKTFileOpt);

        // tioa
        // Controls whether tables are written with Sparql Sql or HiveQL
        final Option HiveQLOpt = new Option("hiveql", "HiveQL", false,
                "Option for using HiveQL instead of Spark SQL for storing tables.");
        HiveQLOpt.setRequired(false);
        options.addOption(HiveQLOpt);

        final Option lpOpt = new Option("lp", "logicalPartitionStrategies", true, "Logical Partition Strategy.");
        lpOpt.setRequired(false);
        options.addOption(lpOpt);

        final Option helpOpt = new Option("h", "help", false, "Print this help.");
        options.addOption(helpOpt);

        final Option statsOpt = new Option("s", "stats", false, "Flag to produce the statistics");
        options.addOption(statsOpt);

        final Option duplicatesOpt = new Option("dp", "dropDuplicates", true,
                "Option to remove duplicates from all logical partitioning tables.");
        options.addOption(duplicatesOpt);

        // Settings for physically partitioning some of the tables
        final Option ttpPartPredOpt = new Option("ttp", "ttPartitionedByPredicate", false,
                "To physically partition the Triple Table by predicate.");
        ttpPartPredOpt.setRequired(false);
        options.addOption(ttpPartPredOpt);

        final Option ttpPartSubOpt
                = new Option("tts", "ttPartitionedBySub", false, "To physically partition the Triple Table by subject.");
        ttpPartSubOpt.setRequired(false);
        options.addOption(ttpPartSubOpt);

        final Option wptPartSubOpt = new Option("wpts", "wptPartitionedBySub", false,
                "To physically partition the Wide Property Table by subject.");
        wptPartSubOpt.setRequired(false);
        options.addOption(wptPartSubOpt);

        final Option outputTripleTable = new Option("ott", "outTripleTable", true,
                "name of triple table output (only for tripleTable)");
        outputTripleTable.setRequired(false);
        options.addOption(outputTripleTable);
        
        // tioa
        final Option hiveTableFormatOpt = new Option("tblfrm", "hiveTableFormat", true,
                "Hive default table format.");
        hiveTableFormatOpt.setRequired(false);
        options.addOption(hiveTableFormatOpt);
        

        final HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
        } catch (final MissingOptionException e) {
            formatter.printHelp("JAR", "Load an RDF graph", options, "", true);
            return;
        } catch (final ParseException e) {
            e.printStackTrace();
        }

        if (cmd.hasOption("help")) {
            formatter.printHelp("JAR", "Load an RDF graph as Property Table using SparkSQL", options, "", true);
            return;
        }
        if (cmd.hasOption("input")) {
            input_location = cmd.getOptionValue("input");
            logger.info("Input path set to: " + input_location);
        }
        if (cmd.hasOption("output")) {
            outputDB = cmd.getOptionValue("output");
            logger.info("Output database set to: " + outputDB);
        }

        // tioa
        // if "dropdb" is missing then the default behaviour is:
        // DB does not exist, therefore create it!
        if (cmd.hasOption("dropdb")) {
            dropDB = Boolean.parseBoolean(cmd.getOptionValue("dropdb"));
            if (dropDB) { // DB exists, but I want to re-create it!
                flagDBExists = true;
                flagCreateDB = true;
                logger.info("Drop database " + outputDB);
            } else {    // DB exists, but I want to use it,
                // probably produce extra information based on existing
                // tables!
                flagDBExists = true;
                flagCreateDB = false;
                logger.info("Use existing database " + outputDB);
            }
        }
        if (cmd.hasOption("statisticsfile")) {
            statFile = cmd.getOptionValue("statisticsfile");
            logger.info("Statistics file: " + statFile);
        }
        if (cmd.hasOption("dictionaryfile")) {
            dictionaryFile = cmd.getOptionValue("dictionaryfile");
            logger.info("Dictionary file: " + dictionaryFile);
        }

        // tioa
        if (cmd.hasOption("namespaceprefixfile")) {
            namespacePrefixFile = cmd.getOptionValue("namespaceprefixfile");
            nsPrefixDictEncode = true;
            logger.info("Namespace Prefix Filename: " + namespacePrefixFile);
        }

        // tioa
        if (cmd.hasOption("propAsWKT")) {
            asWKTFile = cmd.getOptionValue("propAsWKT");
            logger.info("#asWKT properties file: " + asWKTFile);
        }

        // tioa
        if (cmd.hasOption("HiveQL")) {
            useHiveQL_TableCreation = true; // use HiveQL for table creation
            logger.info("Using HiveQL instead of Spark SQL for storing tables");
        }

        // tioa
        if (cmd.hasOption("hiveTableFormat")) {
            hiveTableFormat = cmd.getOptionValue("hiveTableFormat");
            logger.info("Hive default table format: " + hiveTableFormat);
        }

        //addition for DH
        if (cmd.hasOption("outTripleTable")) {
            tttschema.setTblname(cmd.getOptionValue("outTripleTable"));
        }
        gttschema.setTblname("g_".concat(tttschema.getTblname()));

        // default if a logical partition is not specified is: TT, WPT, and VP.
        if (!cmd.hasOption("logicalPartitionStrategies")) {
//            generateTT = true;
//            generateWPT = true;
//            generateVP = true;
//            generateDEC = true;
//            logger.info("Logical strategy used: TT + WPT + VP");
        } else {
            lpStrategies = cmd.getOptionValue("logicalPartitionStrategies");

            final List<String> strategies = Arrays.asList(lpStrategies.split(","));

            if (strategies.contains("TT")) {
                generateTT = true;
                logger.info("Logical strategy used: TT");
            }
            if (strategies.contains("VP")) {
                generateVP = true;
                logger.info("Logical strategy used: VP");
            }
            if (strategies.contains("EXTVP")) {
                generateVP = true;
                generateExtVP = true;
                logger.info("Logical strategy used: VP");
            }
            if (strategies.contains("DEC")) {
                generateDEC = true;
                logger.info("Logical strategy used: DEC");

            }
            if (strategies.contains("PEC")) {
                generatePEC = true;
                logger.info("Logical strategy used: PEC");
            }
        }

        // Relevant for physical partitioning
        if (cmd.hasOption("ttPartitionedByPredicate") && cmd.hasOption("ttPartitionedBySub")) {
            logger.error("Triple table cannot be partitioned by both subject and predicate.");
            return;
        }
        if (cmd.hasOption("ttPartitionedByPredicate")) {
            ttPartitionedByPred = true;
            logger.info("Triple Table will be partitioned by predicate.");
        }
        if (cmd.hasOption("ttPartitionedBySub")) {
            ttPartitionedBySub = true;
            logger.info("Triple Table will be partitioned by subject.");
        }

        // The defaulf value of dropDuplicates is true, so this needs to be
        // changed just in case user sets it as false.
        if (cmd.hasOption("dropDuplicates")) {
            final String dropDuplicateValue = cmd.getOptionValue("dropDuplicates");
            if (dropDuplicateValue.compareTo("false") == 0) {
                dropDuplicates = false;
            }
            logger.info("Duplicates won't be removed from the tables.");
        }

        if (cmd.hasOption("stats")) {
            useStatistics = true;
            logger.info("Statistics active!");

            if (!generateVP) {
                logger.info("Logical strategy activated: VP. VP needed to generate statistics.");
                generateVP = true;
            }
        }

        final SparkSession spark = SparkSession.builder().appName(appName)
                .enableHiveSupport().getOrCreate();
        spark.sql("SET hive.default.fileformat=" + hiveTableFormat);
        
        long startTime;
        long executionTime;

        if (generateTT) {
            startTime = System.currentTimeMillis();
            final TripleTableLoader tt_loader
                    = new TripleTableLoader(spark, outputDB,
                            flagDBExists, flagCreateDB,
                            input_location, false,
                            ttPartitionedBySub, ttPartitionedByPred,
                            dropDuplicates, tttschema, gttschema,
                            namespacePrefixFile, nsPrefixDictEncode,
                            useHiveQL_TableCreation,
                            asWKTFile);
            tt_loader.load();
            flagDBExists = tt_loader.isFlagDBExists();
            flagCreateDB = tt_loader.isFlagCreateDB();
            executionTime = System.currentTimeMillis() - startTime;
            logger.info("Time in ms to build the Tripletable: " + String.valueOf(executionTime));
        }

//        if (generateDEC) {
//            startTime = System.currentTimeMillis();
//            final DictionaryEncoder de_loader
//                    = new DictionaryEncoder(input_location, outputDB, spark, flagDBExists, flagCreateDB,
//                            tripleTable);
//            de_loader.load();
//            
//            executionTime = System.currentTimeMillis() - startTime;
//            logger.info("Time in ms to build the Tripletable: " + String.valueOf(executionTime));
//        }
//        
//        if (generatePEC) {
//            startTime = System.currentTimeMillis();
//            final PrefixEncoder pe_loader
//                    = new PrefixEncoder(input_location, outputDB, spark, flagDBExists, flagCreateDB,
//                            tripleTable);
//            pe_loader.load();
//            
//            executionTime = System.currentTimeMillis() - startTime;
//            logger.info("Time in ms to build the Tripletable: " + String.valueOf(executionTime));
//        }
        if (generateVP) {
            startTime = System.currentTimeMillis();
            final VerticalPartitioningLoader vp_loader
                    = new VerticalPartitioningLoader(spark, outputDB,
                            flagDBExists, flagCreateDB,
                            input_location, false,
                            ttPartitionedBySub, ttPartitionedByPred,
                            dropDuplicates, tttschema, gttschema,
                            namespacePrefixFile, nsPrefixDictEncode,
                            useHiveQL_TableCreation,
                            asWKTFile,
                            dictionaryFile, generateExtVP, thresholdExtVP);
            vp_loader.load();
            executionTime = System.currentTimeMillis() - startTime;
            logger.info("Time in ms to build the Vertical partitioning: " + String.valueOf(executionTime));
        }
        logger.info("Total time: " + (System.currentTimeMillis() - start));
    }
}