package loader2.utils;

import java.io.Serializable;
import java.util.List;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import run2.Main;

/**
 *
 * @author tioannid
 */
public class NamespaceDictionary implements Serializable  {

    // ----- STATIC MEMBERS -----
    protected static final Logger logger = Logger.getLogger(Main.appName);
    // TODO: Do we expect to have more than one NamespaceDictionary?
    public static long cnt = 0;

    // ----- STATIC METHODS -----
    public static void printUsageStatistics() {
        logger.info("No of dictionaries :\t\t" + NamespaceDictionary.cnt);
    }

    // ----- DATA MEMBERS -----
    private final SparkSession spark;
    private final String namespacePrefixJSONFile;  // JSON file with namespace prefixes
    private final List<Namespace> nsList;
    private final String nsPrefTableName; // Hive table to store namespace prefixes

    // ----- CONSTRUCTORS -----
    /**
     * Read from the HDFS JSON {@link namespacePrefixFile} the namespace prefixes
     * and persisted them to the table {@link nsPrefTableName}
     * 
     * @param spark
     * @param namespacePrefixFile A string representing the HDFS JSON file with
     *          the namespace prefixes
     * @param nsPrefTableName A String with the table name where the namespace
     *          prefixes are to be stored
     */
    public NamespaceDictionary(SparkSession spark, String namespacePrefixFile,
            String nsPrefTableName) {
        this.spark = spark;
        this.namespacePrefixJSONFile = namespacePrefixFile;
        this.nsPrefTableName = nsPrefTableName;
        Dataset<Namespace> prefixDS = spark.read().json(namespacePrefixFile).as(Namespace.Encoder);
        this.nsList = prefixDS.collectAsList();
        prefixDS.write().saveAsTable(nsPrefTableName);
        cnt++; // increment the number of dictionaries created
    }

    // ----- DATA ACCESSORS -----
    public SparkSession getSpark() {
        return spark;
    }

    public String getNamespacePrefixFile() {
        return namespacePrefixJSONFile;
    }

    public String getNsPrefTableName() {
        return nsPrefTableName;
    }

    public List<Namespace> getNsList() {
        return nsList;
    }

    // ----- METHODS -----
    public String getTurtlePrefixHeader() {
        StringBuilder sb = new StringBuilder();
        for (Namespace ns : this.nsList) {
            sb.append(ns.getTurtlePrefixLine());
        }
        return sb.toString();
    }
}