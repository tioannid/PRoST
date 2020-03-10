package loader;


import org.apache.spark.sql.SparkSession;

public class DictionaryEncoder extends Loader {

    private String tableName;
    private String dbName;
    private String tableSpecifier;
    private SparkSession spark;
    private String newName;
    private String finalTable;


    final private String mixedTable = "mixed_temp";
    final private String distinctTable = "distinct_temp";
    final private String keyPrefix = "dd";
    final private String rowedTable = "rowed_temp";
    final private String final1 = "final1";

    final private boolean removeTempTables = false;


    public DictionaryEncoder(String hdfs_directory ,String database, String table, SparkSession spark, String newName) {
        super(hdfs_directory, database, spark);
        this.tableName = table;
        this.dbName = database;
        this.spark = spark;
        this.tableSpecifier = String.format("%1$s.%2$s", dbName, tableName);
        if (this.newName == null) {
            this.newName = "encoded";
        }
        finalTable = String.format("%1$s_%2$s", this.tableName, this.newName);

    }

    public void load() {

        String dropTable = String.format("DROP TABLE IF EXISTS %1$s", tableSpecifier);
        String dropFinalTable = String.format("DROP TABLE IF EXISTS %1$s.%2$s", dbName, finalTable);
        String drop1 = String.format("DROP TABLE IF EXISTS %1$s.%2$s", dbName, finalTable);
        String drop2 = String.format("DROP TABLE IF EXISTS %1$s.%2$s", dbName, mixedTable);
        String drop3 = String.format("DROP TABLE IF EXISTS %1$s.%2$s", dbName, distinctTable);
        String drop4 = String.format("DROP TABLE IF EXISTS %1$s.%2$s", dbName, final1);


        spark.sql(dropTable);
        spark.sql(dropFinalTable);
        spark.sql(drop1);
        spark.sql(drop2);
        spark.sql(drop3);
        spark.sql(drop4);



        String insertSubjects = String.format("CREATE TABLE %1$s.2$s  AS SELECT %3$s %4$s",
                dbName, mixedTable, column_name_subject,tableSpecifier);
        String insertObjects = String.format("INSERT INTO TABLE %1$s.2$s  AS SELECT %3$s %4$s WHERE OType=2",
                dbName, mixedTable, column_name_object,tableSpecifier);
        String createDistinctTable = String.format("CREATE TABLE %1$s.%2$s  AS SELECT DISTINCT(s) FROM %1$s.%4$s",
                dbName, distinctTable, mixedTable);
        String addRowNumber = String.format("CREATE TABLE %1$s.%2$s  AS SELECT s as key, concat('%3$s', (row_number() over (order by s))) value FROM %1$s.%4$s",
                dbName, rowedTable, keyPrefix, distinctTable);
        String createFinalPart1 = String.format("CREATE TABLE %1$s.%2$s as SELECT t1.value as s,t.p,t2.value as o , t.OType=2 FROM prost_test.dict_final t1, prost_test.dict_final t2, prost_test.triples t WHERE t1.key=t.s AND t2.key=t.o"
                ,dbName, final1);
        String createFinal = String.format("CREATE TABLE %1$s.%2$s AS (SELECT * FROM %1$s.%3$s UNION SELECT * FROM %1$s.%4$s WHERE OType=1)",
                dbName, finalTable, final1, tableName);



        spark.sql(insertSubjects);
        spark.sql(insertObjects);
        spark.sql(createDistinctTable);
        spark.sql(addRowNumber);
        spark.sql(createFinalPart1);
        spark.sql(createFinal);

        if (this.removeTempTables) {
            spark.sql(drop1);
            spark.sql(drop2);
            spark.sql(drop3);
            spark.sql(drop4);
            spark.sql(dropTable);

        }

    }

}