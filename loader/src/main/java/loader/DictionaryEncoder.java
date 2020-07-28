package loader;

import org.apache.spark.sql.SparkSession;

public class DictionaryEncoder extends Loader {

    private String tableName;
    private String dbName;
    private String tableSpecifier;
    private SparkSession spark;
    private String newName;

    final private String mixedTable = "mixed_temp";
    final private String distinctTable = "distinct_temp";
    final private String keyPrefix = "dd";
    final private String rowedTable = "dictionary_so";
    final private String final1 = "final1";

    final private boolean removeTempTables = false;

    // CONSTRUCTORS
    public DictionaryEncoder(String hdfs_input_directory, String database_name, SparkSession spark,
            boolean flagDBExists, boolean flagCreateDB,
            String table, String newName) {
        super(hdfs_input_directory, database_name, spark, flagDBExists, flagCreateDB);
        this.dbName = database_name;
        this.spark = spark;
        this.tableName = table;
        this.tableSpecifier = String.format("%1$s.%2$s", dbName, tableName);
        if (this.newName == null) {
            this.newName = "encoded";
        }
    }

    public DictionaryEncoder(String hdfs_input_directory, String database_name, SparkSession spark,
            boolean flagDBExists, boolean flagCreateDB,
            String table) {
        this(hdfs_input_directory, database_name, spark, flagDBExists, flagCreateDB,
                table, "encoded");
    }

    // DATA ACCESSORS   
    public String getFinalTable() {
        return String.format("%1$s_%2$s", this.tableName, this.newName);

    }

    public void load() {
        logger.info("----- Starting Dictionary Encoder ------");

        String dropFinalTable = String.format("DROP TABLE IF EXISTS %1$s.%2$s", dbName, this.getFinalTable());
        String drop1 = String.format("DROP TABLE IF EXISTS %1$s.%2$s", dbName, rowedTable);
        String drop2 = String.format("DROP TABLE IF EXISTS %1$s.%2$s", dbName, mixedTable);
        String drop3 = String.format("DROP TABLE IF EXISTS %1$s.%2$s", dbName, distinctTable);
        String drop4 = String.format("DROP TABLE IF EXISTS %1$s.%2$s", dbName, final1);

        spark.sql(dropFinalTable);
        spark.sql(drop1);
        spark.sql(drop2);
        spark.sql(drop3);
        spark.sql(drop4);

        String insertSubjects = String.format("CREATE TABLE %1$s.%2$s  AS (SELECT %3$s FROM %4$s)",
                dbName, mixedTable, column_name_subject, tableSpecifier);
        String insertObjects = String.format("INSERT INTO TABLE %1$s.%2$s SELECT %3$s FROM %4$s WHERE OType=2",
                dbName, mixedTable, column_name_object, tableSpecifier);
        String createDistinctTable = String.format("CREATE TABLE %1$s.%2$s  AS (SELECT DISTINCT(s) FROM %1$s.%3$s)",
                dbName, distinctTable, mixedTable);
        String addRowNumber = String.format("CREATE TABLE %1$s.%2$s  AS (SELECT s as key, concat('%3$s', (row_number() over (order by s))) value FROM %1$s.%4$s)",
                dbName, rowedTable, keyPrefix, distinctTable);
        String createFinalPart1 = String.format("CREATE TABLE %1$s.%2$s as (SELECT * FROM %1$s.%3$s UNION (select o, o FROM %1$s.%4$s WHERE OType=1))",
                dbName, final1, rowedTable, tableName);
        String createFinal = String.format("CREATE TABLE %1$s.%2$s as (SELECT t1.value as s,t.p,t2.value as o, t.OType FROM %1$s.%3$s t1, %1$s.%3$s t2, %1$s.%4$s t WHERE t1.key=t.s AND t2.key=t.o)",
                 dbName, this.getFinalTable(), final1, tableName);

        logger.info("---- DEC ----- Running the following:");
        logger.info(insertSubjects);
        logger.info(insertObjects);
        logger.info(createDistinctTable);
        logger.info(addRowNumber);
        logger.info(createFinalPart1);
        logger.info(createFinal);

        spark.sql(insertSubjects);
        spark.sql(insertObjects);
        spark.sql(createDistinctTable);
        spark.sql(addRowNumber);
        spark.sql(createFinalPart1);
        spark.sql(createFinal);

        if (this.removeTempTables) {
            //spark.sql(drop1); Never drop this one, as it contains the encoding
            spark.sql(drop2);
            spark.sql(drop3);
            spark.sql(drop4);

        }

    }

}
