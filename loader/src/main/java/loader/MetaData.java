package loader;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


//class that generates metadata for the vertical partitioning store
//the metadata table is as follows:
// VP TABLE NAME - District O - Distinct S
public class MetaData {
	protected boolean discoverTables = true;
    protected String dbname = "prost_test";
    SparkSession spark;

    protected String tableName = "vpmetadata";

    protected String table_name_column = "tn";
    protected String distinct_objects_column = "do";
    protected String distinct_subjects_column = "ds";


    public MetaData(final SparkSession spark) {
        this.spark = spark;
    }

    public void generateMetaData() {
        String vpTableName = dbname+this.tableName;

        final String queryDropTripleTable = String.format("DROP TABLE IF EXISTS %s", vpTableName);
        spark.sql(queryDropTripleTable);

        String createMetaDataTable = String.format(
            "CREATE TABLE IF NOT EXISTS %1$s(%2$s STRING, %3$s INT, %4$s INT)", vpTableName,
            table_name_column, distinct_subjects_column, distinct_objects_column);
        spark.sql(createMetaDataTable);



        //discover tables here
        Dataset<Row> ds = spark.sql("show tables from prost_test");
        List<Row> listofTables = spark.sql("show tables from prost_test").takeAsList((int)ds.count());
        for (int i=0; i < listofTables.size()-1; i++) {
            String table = listofTables.get(i).getString(1);
            System.out.println(table);
            if (!table.startsWith("vp"))
                continue;

            String query = String.format("SELECT COUNT(DISTINCT s),COUNT(DISTINCT o) FROM prost_test.%s", table);
            Dataset<Row> dd = spark.sql(query);
            dd.show();
            long distinctSubjects = dd.first().getLong(0);
            long distinctObjects = dd.first().getLong(1);

            query = String.format(
                "INSERT INTO %1$s VALUES( %2$s, %3$s, %4$s )",
                vpTableName, table, distinctSubjects, distinctObjects);
            System.out.println(query);
            //spark.sql(query);
            
        }
        

        //print the results to see that they are correct
        spark.sql("select * from "+vpTableName).show();

    }


    
}