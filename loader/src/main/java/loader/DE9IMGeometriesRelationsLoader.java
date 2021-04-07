package loader;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.collection.JavaConverters;
import scala.collection.Seq;



public class DE9IMGeometriesRelationsLoader extends Loader{
	private String relations_filepath;
	
	
	public DE9IMGeometriesRelationsLoader(String hdfs_input_directory, String database_name, SparkSession spark,
			String relations_filepath) {
		super(hdfs_input_directory, database_name, spark);
		this.relations_filepath = relations_filepath;
	}

	@Override
	public void load() throws Exception {
		
        JavaSparkContext sparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
//following creates 9 different tables , one for each relation
//        try {
//    		//read relations file and create the dataframe 
//        	DE9IMGeometriesRelationsParser par = new DE9IMGeometriesRelationsParser();
//            JavaRDD<String> inputFile = sparkContext.textFile(relations_filepath);
//            JavaRDD<DE9IM> parsedStatements = inputFile.map(line -> par.parseline(line));
//            Dataset<Row> dataset = spark.createDataFrame(parsedStatements, DE9IM.class);
//            
//            //all relations
//            List<DE9IMEnum> relations = new ArrayList<>();
//            relations.add(DE9IMEnum.Contains);
//            relations.add(DE9IMEnum.CoveredBy);
//            relations.add(DE9IMEnum.Covers);
//            relations.add(DE9IMEnum.Crosses);
//            relations.add(DE9IMEnum.Equals);
//            relations.add(DE9IMEnum.Intersects);
//            relations.add(DE9IMEnum.Overlaps);
//            relations.add(DE9IMEnum.Touches);
//            relations.add(DE9IMEnum.Within);
//
//            //for each relation
//            for (DE9IMEnum relation : relations) {
//            	//drop relation table if already exists
//    			final String droprelationtable = String.format("DROP TABLE IF EXISTS %s", relation.toString());
//    			spark.sql(droprelationtable);
//    			
//    			//gather columns from dataframe to create relation-level dataframe
//    			List<String> columns = Arrays.asList(DE9IMEnum.id1.toString(),DE9IMEnum.id2.toString(),relation.toString());
//    			Dataset<Row> relationdataframe = dataset.selectExpr(convertListToSeq(columns));
//    			final String temprelationtable = String.format("temp" + relation.toString());
//    			final String finalrelationtable = String.format("tbl" + relation.toString());
//    			relationdataframe.createOrReplaceTempView(temprelationtable);
//    			final String createrelationtable = String.format(
//    					"CREATE TABLE  IF NOT EXISTS %1$s AS SELECT * FROM %2$s",
//    					finalrelationtable,
//    					temprelationtable);
//
//    			spark.sql(createrelationtable);
//            }
//            
//    		
//    		logger.info("Edw mpika Geometries Relations");        	
//        }
//        catch (Exception e){
//        	logger.error("Could not create relations tables: " + e.getMessage());
//        }

        try {
    		//read relations file and create the dataframe 
        	DE9IMGeometriesRelationsParser par = new DE9IMGeometriesRelationsParser();
            JavaRDD<String> inputFile = sparkContext.textFile(relations_filepath);
            JavaRDD<DE9IM> parsedStatements = inputFile.map(line -> par.parseline(line));
            Dataset<Row> dataset = spark.createDataFrame(parsedStatements, DE9IM.class);
            
            //create hive table
            dataset.createOrReplaceTempView("tempde9im");
			final String createrelationtable = String.format(
					"CREATE TABLE  IF NOT EXISTS tblde9im AS SELECT * FROM tempde9im");

			spark.sql(createrelationtable);            
        }
	      catch (Exception e){
	    	logger.error("Could not create relations tables: " + e.getMessage());
	    }
	}

    public static Seq<String> convertListToSeq(List<String> inputList) {
        return JavaConverters.asScalaIteratorConverter(inputList.iterator()).asScala().toSeq();
    }
}
