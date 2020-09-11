package loader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.*;

/**
 * Build the VP, i.e. a table for each predicate.
 *
 * @author Matteo Cossu
 * @author Victor Anthony Arrascue Ayala
 *
 */
public class VerticalPartitioningLoader extends Loader {

    private final boolean computeStatistics;
    private final Map<String, String> predDictionary;
    private String dict_file_name;
    private Map<String, TableInfo> statistics;
    private String metadata_file_name;
    private boolean generateExtVP;
    private double threshold;
    private boolean dictEncoded;

    public VerticalPartitioningLoader(final String hdfs_input_directory, final String database_name,
            final String input_table,
            final SparkSession spark, final boolean computeStatistics, final String statisticsfile,
            final String dictionaryfile, boolean generateExtVP, double thresholdExtVP) {
        super(hdfs_input_directory, database_name, spark);
        this.computeStatistics = computeStatistics;
        this.predDictionary = new HashMap<String, String>();
        this.statistics = new HashMap<String, TableInfo>();
        this.dict_file_name = dictionaryfile;
        this.metadata_file_name = statisticsfile;
        this.generateExtVP = generateExtVP;
        this.threshold = thresholdExtVP;
        this.dictEncoded = true;
        this.name_tripletable = input_table;
    }
    //

    public void createGeometryTable() throws Exception {
        spark.sql("DROP TABLE IF EXISTS geometries");
        Dataset<Row> geometries = spark.sql("Select g.s as entity, ifnull(w.s, g.o) as geom, w.o as wkt from "
                + String.format(" (select * from %s where p='http://www.opengis.net/ont/geosparql#asWKT') w full outer join", name_tripletable)
                + String.format(" (select * from %s where p='http://www.opengis.net/ont/geosparql#hasGeometry') g on ", name_tripletable)
                + " g.o=w.s");
        geometries.createOrReplaceTempView("geometries");
        spark.catalog().cacheTable("geometries");
        //geometries.persist(StorageLevel.MEMORY_AND_DISK());
        geometries.write().saveAsTable("geometries");
        //put as stats for geometries table the hasGeometry stats
        long rows = geometries.count();
        TableInfo tblInfo = new TableInfo(rows, rows, rows);
        statistics.put("geometries", tblInfo);
        //geometries.unpersist();
        spark.catalog().uncacheTable("geometries");
    }

    @Override
    public void load() {
        logger.info("PHASE 3: creating the VP tables on " + this.name_tripletable);

        //get properties name
        if (properties_names == null) {
            properties_names = extractProperties();
        }

        //generate predicate map, map long predicates into smaller names , pred1, pred2...
        try {
            generatePredicateDictionary();
        } catch (IOException e) {
            e.printStackTrace();
        }

        boolean geometriesCreated = true;

        try {
            createGeometryTable();
        } catch (Exception e) {
            geometriesCreated = false;
            logger.error("Could not create geometries table: " + e.getMessage());
        }

        for (int i = 0; i < properties_names.length; i++) {
            if (properties_names[i] == null) {
                continue;
            }
            if (geometriesCreated
                    && (properties_names[i].equals("http://www.opengis.net/ont/geosparql#asWKT")
                    || properties_names[i].equals("http://www.opengis.net/ont/geosparql#hasGeometry"))) {
                //we do not have to create the VP table
                predDictionary.remove(properties_names[i]);
                continue;
            }
            String property;
            property = this.predDictionary.get(properties_names[i]);

            final String queryDropVPTableFixed = String.format("DROP TABLE IF EXISTS %s", property);
            spark.sql(queryDropVPTableFixed);

            final String createVPTableFixed = String.format(
                    "CREATE TABLE  IF NOT EXISTS  %1$s(%2$s STRING, %3$s STRING) STORED AS PARQUET", property,
                    column_name_subject, column_name_object);
            // Commented code is partitioning by subject
            /*
			 * String createVPTableFixed = String.format(
			 * "CREATE TABLE  IF NOT EXISTS  %1$s(%3$s STRING) PARTITIONED BY (%2$s STRING) STORED AS PARQUET"
			 * , "vp_" + this.getValidHiveName(property), column_name_subject,
			 * column_name_object);
             */
            //spark.sql(createVPTableFixed);

            final String populateVPTable = String.format(
                    "INSERT INTO TABLE %1$s " + "SELECT %2$s, %3$s " + "FROM %4$s WHERE %5$s = '%6$s' ", property,
                    column_name_subject, column_name_object, name_tripletable, column_name_predicate,
                    properties_names[i]);
            // Commented code is partitioning by subject
            /*
			 * String populateVPTable = String.format(
			 * "INSERT OVERWRITE TABLE %1$s PARTITION (%2$s) " + "SELECT %3$s, %2$s " +
			 * "FROM %4$s WHERE %5$s = '%6$s' ", "vp_" + this.getValidHiveName(property),
			 * column_name_subject, column_name_object, name_tripletable,
			 * column_name_predicate, property);
             */
            //spark.sql(populateVPTable);
            final String selectVPTable = String.format(
                    "SELECT %1$s, %2$s " + "FROM %3$s WHERE %4$s = '%5$s' ", column_name_subject, column_name_object, name_tripletable, column_name_predicate,
                    properties_names[i]);

            // calculate statspredDictionary
            final Dataset<Row> table_VP = spark.sql(selectVPTable);
            table_VP.createOrReplaceTempView(property);
            spark.catalog().cacheTable(property);
            //final Dataset<Row> table_VP = spark.sql("select * from "+property);
            //table_VP.persist(StorageLevel.MEMORY_AND_DISK());

            table_VP.write().saveAsTable(property);

            if (computeStatistics) {
                statistics.put(property, calculate_stats_table(table_VP, property));
            }

            logger.info("Created VP table for the property: " + property);
            final List<Row> sampledRowsList = table_VP.limit(3).collectAsList();
            logger.info("First 3 rows sampled (or less if there are less): " + sampledRowsList);
            //table_VP.unpersist();
        }

        // done with the vertical paritioning
        // Extract IRIs
        List<String> tablesWithIRIs = new ArrayList<String>();
        for (String tbl : extractPredicatesWithIRIObjects()) {
            if (tbl.equals("http://www.opengis.net/ont/geosparql#hasGeometry")
                    || tbl.equals("http://www.opengis.net/ont/geosparql#asWKT")) {
                //what to do?
                continue;
            }
            tablesWithIRIs.add(predDictionary.get(tbl));
        }
        try {
            logger.info("Saving Information about tables with IRIs or Literals as objects");
            save_to_file(tablesWithIRIs.toString().getBytes(), metadata_file_name + "-tablesWithIRIs.txt");
            //save_to_file(this.tablesWithLiterals.toString().getBytes(), metadata_file_name + "-tablesWithLiterals.txt");
        } catch (Exception e) {
            logger.error("Could not save Information about tables with IRIs or Literals as objects: " + e.getMessage());
        }

        // Compute relevant statistics
        if (computeStatistics) {
            try {
                save_stats();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        logger.info("Vertical Partitioning completed. Loaded " + String.valueOf(properties_names.length) + " tables.");

        if (generateExtVP) {
            spark.catalog().uncacheTable(name_tripletable);
            ExtVPCreator extvp = new ExtVPCreator(predDictionary, spark, threshold, statistics, tablesWithIRIs);
            logger.info("Creating SS ExtVP");
            extvp.createExtVP("SS");
            logger.info("Creating SO ExtVP");
            extvp.createExtVP("SO");
            logger.info("Creating OS ExtVP");
            extvp.createExtVP("OS");
            try {
                logger.info("Saving ExtVP Statistics");
                save_to_file(extvp.getExtVPStats().toString().getBytes(), metadata_file_name + ".ext");
            } catch (Exception e) {
                logger.error("Could not save ExtVP statistics: " + e.getMessage());
            }
        }
        /*
		 * try { Loader.parseCSVDictionary(dict_file_name); } catch (IOException e) {
		 * e.printStackTrace(); }
         */

        logger.info("properties with objects that are IRIs: " + tablesWithIRIs.toString());

        /*Set<String> intersection = new HashSet<String>(tablesWithIRIs); // use the copy constructor
		intersection.retainAll(tablesWithLiterals);
		if(!intersection.isEmpty()) {
			logger.error("properties that have both IRIs and literals as objects: "+intersection.toString());
		}*/
    }

    /*
	 * calculate the statistics for a single table: size, number of distinct
	 * subjects and isComplex. It returns a protobuf object defined in
	 * ProtobufStats.proto
     */
    private TableInfo calculate_stats_table(final Dataset<Row> table, final String tableName) {

        // calculate the stats
        final long table_size = table.count();
        final long distinct_subjects = table.select(column_name_subject).distinct().count();
        final long distinct_objects = table.select(column_name_object).distinct().count();
        logger.info("table:" + tableName + " has " + table_size + " rows");
        TableInfo tblInfo = new TableInfo(table_size, distinct_subjects, distinct_objects);

        return tblInfo;
    }

    /*
	 * save the statistics in a serialized file
     */
    private void save_stats() throws IllegalArgumentException, IOException {
        logger.info("Saving Statistics");
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        FSDataOutputStream out = fs.create(new Path(metadata_file_name));
        for (String tablename : statistics.keySet()) {
            TableInfo ti = statistics.get(tablename);
            String fmt = String.format("%1$s,%2$s,%3$s,%4$s\n", tablename, ti.getCountAll(), ti.getDistinctSubjects(),
                    ti.getDistinctObjects());
            byte[] bytes = fmt.getBytes();
            out.write(fmt.getBytes(), 0, bytes.length);
        }
        out.close();

    }

    private void save_to_file(byte[] bs, String filename) throws IllegalArgumentException, IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        FSDataOutputStream out = fs.create(new Path(filename));
        out.write(bs);
        out.close();

    }

    private String[] extractProperties() {
        final List<Row> props = spark
                .sql(String.format("SELECT DISTINCT(%1$s) AS %1$s FROM %2$s", column_name_predicate, name_tripletable))
                .collectAsList();
        final String[] properties = new String[props.size()];

        for (int i = 0; i < props.size(); i++) {
            properties[i] = props.get(i).getString(0);
        }

        final List<String> propertiesList = Arrays.asList(properties);
        logger.info("Number of distinct predicates found: " + propertiesList.size());
        logger.info("Predicates: " + propertiesList.toString());
        final String[] cleanedProperties = handleCaseInsPred(properties);
        final List<String> cleanedPropertiesList = Arrays.asList(cleanedProperties);
        logger.info("Final list of predicates: " + cleanedPropertiesList);
        logger.info("Final number of distinct predicates: " + cleanedPropertiesList.size());
        return cleanedProperties;
    }

    private List<String> extractPredicatesWithIRIObjects() {
//		if (this.dictEncoded)
//			return new ArrayList<String>();	//return an empty list as there are no items

        String sql = "select distinct " + column_name_predicate + " from "
                + name_tripletable + " where " + column_name_object_type + " = 2 ";
        return spark.sql(sql).as(Encoders.STRING()).collectAsList();
    }

    private String[] handleCaseInsPred(final String[] properties) {
        final Set<String> seenPredicates = new HashSet<>();
        final Set<String> originalRemovedPredicates = new HashSet<>();

        final Set<String> propertiesSet = new HashSet<>(Arrays.asList(properties));

        final Iterator<String> it = propertiesSet.iterator();
        while (it.hasNext()) {
            final String predicate = it.next();
            logger.info("Next Predicate: " + predicate);
            if (predicate == null) {
                logger.error("null predicate encountered");
                continue;
            }
            if (seenPredicates.contains(predicate.toLowerCase())) {
                originalRemovedPredicates.add(predicate);
            } else {
                seenPredicates.add(predicate.toLowerCase());
            }
        }

        for (final String predicateToBeRemoved : originalRemovedPredicates) {
            propertiesSet.remove(predicateToBeRemoved);
        }

        if (originalRemovedPredicates.size() > 0) {
            logger.info("The following predicates had to be removed from the list of predicates "
                    + "(it is case-insensitive equal to another predicate): " + originalRemovedPredicates);
        }
        final String[] cleanedProperties = propertiesSet.toArray(new String[propertiesSet.size()]);
        return cleanedProperties;
    }

    /**
     * Checks if there is at least one property that uses prefixes.
     */
    private boolean arePrefixesUsed() {
        for (final String property : properties_names) {
            if (property.contains(":")) {
                return true;
            }
        }
        return false;
    }

    /**
     * Additions by D Halatsis Create a csv file that contains predicates and
     * their new names
     */
    private void generatePredicateDictionary() throws IOException {
        if (properties_names == null) {
            properties_names = extractProperties();
        }
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        FSDataOutputStream out = fs.create(new Path(dict_file_name));

        for (int i = 0; i < properties_names.length; i++) {
            predDictionary.put(properties_names[i], "prop" + i);
            String fmt = String.format("%1$s,prop%2$s\n", properties_names[i], i + "");
            byte[] bytes = fmt.getBytes();
            out.write(fmt.getBytes(), 0, bytes.length);
        }
        out.close();
    }

}
