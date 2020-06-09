package loader;

import org.apache.spark.api.java.AbstractJavaRDDLike;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PrefixEncoder extends Loader {

    private String tableName;
    private String dbName;
    private SparkSession spark;
    private String newName;
    private String finalTable;
    private boolean halfencode = true;

    public PrefixEncoder(String hdfs_directory , String database, String table, SparkSession spark, String newName) {
        super(hdfs_directory, database, spark);
        this.tableName = table;
        this.dbName = database;
        this.spark = spark;
        if (this.newName == null) {
            this.newName = "pr_encoded";
        }
        finalTable = String.format("%1$s_%2$s", this.tableName, this.newName);

    }

    public static List<String> splitURI(Row item) {
        String URI = item.getString(0);
        if (!URI.startsWith("http"))
            return new ArrayList<String>();
        List<String> arr = Arrays.asList(URI.split("/", 0));
        //TODO comment this!
        //Collections.reverse(arr);
        return arr;

    }
    private void createDistinct() {
        spark.sql(String.format("DROP TABLE IF EXISTS %1$s.triples_distinct", this.dbName));
        spark.sql(String.format("DROP TABLE IF EXISTS %1$s.td1", this.dbName));
        spark.sql(String.format("DROP TABLE IF EXISTS %1$s.td2", this.dbName));


        spark.sql(String.format("CREATE TABLE %1$s.td1  AS (SELECT s FROM %1$s.%2$s)", this.dbName, this.tableName));
        spark.sql(String.format("INSERT INTO %1$s.td1 (SELECT o FROM %1$s.%2$s WHERE OType=2)", this.dbName, this.tableName));
        spark.sql(String.format("CREATE TABLE %1$s.triples_distinct AS (SELECT DISTINCT(s) FROM %1$s.td1)", this.dbName));
    }



    public static String reconURI(List<String> ls, int start, int end, String prefix) {
        int i;
        for (i = start; i < end && i < ls.size() - 1; i++) {
            prefix = prefix + ls.get(i) + "/";
        }
        if (i < ls.size())
            return prefix + ls.get(i);
        return prefix;
    }


    public void load() {
        createDistinct();
        Dataset<Row> res;
        res = spark.sql(String.format("SELECT * FROM %1$s.triples_distinct", this.dbName));

        JavaRDD<Row> data = res.toJavaRDD();


        JavaRDD<URITable> list = data.map(item -> new URITable(item));
        Dataset<Row> dataset = spark.createDataFrame(list, URITable.class);
        dataset.createOrReplaceTempView("tempTable");

        System.out.println(5);
        spark.sql(String.format("DROP TABLE IF EXISTS %1$s.triples_split", this.dbName));
        spark.sql(String.format("CREATE TABLE IF NOT EXISTS %1$s.triples_split AS SELECT * FROM tempTable", this.dbName));


        spark.sql(String.format("DROP TABLE IF EXISTS %1$s.right_dict", this.dbName));
        spark.sql(String.format("DROP TABLE IF EXISTS %1$s.left_dict", this.dbName));
        spark.sql(String.format("DROP TABLE IF EXISTS %1$s.tripl_left", this.dbName));
        spark.sql(String.format("DROP TABLE IF EXISTS %1$s.tripl_right", this.dbName));
        spark.sql(String.format("DROP TABLE IF EXISTS %1$s.mixed", this.dbName));
        spark.sql(String.format("DROP TABLE IF EXISTS %1$s.double_encoded", this.dbName));



        spark.sql(String.format("CREATE TABLE IF NOT EXISTS %1$s.tripl_left AS SELECT DISTINCT left from %1$s.triples_split", this.dbName));
        spark.sql(String.format("CREATE TABLE IF NOT EXISTS %1$s.tripl_right AS SELECT DISTINCT right from %1$s.triples_split", this.dbName));

        spark.sql(String.format("CREATE TABLE %1$s.left_dict  AS (SELECT left as key, concat('l', (row_number() over (order by left))) value FROM %1$s.tripl_left)", this.dbName));
        spark.sql(String.format("CREATE TABLE %1$s.right_dict  AS (SELECT right as key, concat('r', (row_number() over (order by right))) value FROM %1$s.tripl_right)", this.dbName));

        spark.sql(String.format("CREATE TABLE %1$s.mixed AS (SELECT t.full, t.left, l.value as lvalue, t.right, r.value as rvalue FROM %1$s.triples_split t, %1$s.left_dict l, %1$s.right_dict r WHERE t.left = l.key AND t.right = r.key)", this.dbName));
        spark.sql(String.format("DROP TABLE IF EXISTS %1$s.%2$s", this.dbName, this.finalTable));
        spark.sql(String.format("DROP TABLE IF EXISTS %1$s.double_dictionary", this.dbName));
        if (halfencode)
            spark.sql(String.format("CREATE TABLE IF NOT EXISTS %1$s.double_dictionary AS ((SELECT concat(concat(lvalue, '/'), right) as value, full as key  FROM %1$s.mixed UNION (SELECT o,o FROM %1$s.triples WHERE Otype=1)))", this.dbName));
        else
            spark.sql(String.format("CREATE TABLE IF NOT EXISTS %1$s.double_dictionary AS ((SELECT concat(concat(lvalue, '/'), rvalue) as value, full as key  FROM %1$s.mixed UNION (SELECT o,o FROM %1$s.triples WHERE Otype=1)))", this.dbName));

        spark.sql(String.format("CREATE TABLE IF NOT EXISTS %1$s.%2$s AS SELECT m1.value as s,t.p, m2.value as o, t.otype FROM %1$s.double_dictionary m1, %1$s.double_dictionary m2, %1$s.triples t WHERE m1.key=t.s AND m2.key=t.o  ", this.dbName, this.finalTable));

    }
}
