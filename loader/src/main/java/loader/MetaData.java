package loader;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
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

    final private String metadata_file_name = "hdfs:///Projects/prost_test/Resources/vp_metadata.csv";


    public MetaData(final SparkSession spark) {
        this.spark = spark;
    }

    public void generateMetaData() throws IOException{
        String vpTableName = dbname+'.'+this.tableName;



        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        FSDataOutputStream out = fs.create(new Path(metadata_file_name));


        //discover tables here
        Dataset<Row> ds = spark.sql("show tables from prost_test");
        List<Row> listofTables = spark.sql("show tables from prost_test").takeAsList((int)ds.count());
        for (int i=0; i < listofTables.size()-1; i++) {
            String table = listofTables.get(i).getString(1);
            System.out.println(table);
            if (!table.startsWith("vp_"))
                continue;

            String query = String.format("SELECT COUNT(DISTINCT s),COUNT(DISTINCT o) FROM prost_test.%s", table);
            Dataset<Row> dd = spark.sql(query);
            dd.show();
            long distinctSubjects = dd.first().getLong(0);
            long distinctObjects = dd.first().getLong(1);


            String fmt = String.format("%1$s,%2$s,%3$s\n", table, distinctSubjects, distinctObjects);
            byte[] bytes = fmt.getBytes();
            out.write(fmt.getBytes(), 0, bytes.length);

        }
        out.close();




    }

    public HashMap<String,  String[]> parseMetadata() throws IOException{
        FileSystem fs = FileSystem.get(new Configuration());
        FSDataInputStream in = fs.open(new Path(metadata_file_name));

        Scanner scanner = new Scanner(in);
        scanner.useDelimiter(",");

        HashMap<String, String[]> map = new HashMap<>();

        while (scanner.hasNext()) {
            String key = scanner.next();
            String value1 = scanner.next();
            String value2 = scanner.next();
            map.put(key, new String[]{value1, value2});

        }
        scanner.close();

        //print the map
        for (String key : map.keySet()) {
            String val = map.get(key).toString();
            System.out.println(val);
        }
        return map;

    }


    
}