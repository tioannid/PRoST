package loader;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


/**
 * Class that constructs a triples table. First, the loader creates an external
 * table ("raw"). The data is read using SerDe capabilities and by means of a
 * regular expresion. An additional table ("fixed") is created to make sure that
 * only valid triples are passed to the next stages in which other models e.g.
 * Property Table, or Vertical Partitioning are built.
 *
 * @author Matteo Cossu
 * @author Victor Anthony Arrascue Ayala
 */
public class TripleTableLoader extends Loader {
	protected boolean ttPartitionedBySub = false;
	protected boolean ttPartitionedByPred = false;
	protected boolean dropDuplicates = true;
	protected boolean useRDFLoader = true;
	protected boolean onlyGenerateMetadata = true;
	

	public TripleTableLoader(final String hdfs_input_directory, final String database_name, final String output_table,final SparkSession spark,
			final boolean ttPartitionedBySub, final boolean ttPartitionedByPred, final boolean dropDuplicates) {
		super(hdfs_input_directory, database_name, spark);
		this.name_tripletable = output_table;
		this.ttPartitionedBySub = ttPartitionedBySub;
		this.ttPartitionedByPred = ttPartitionedByPred;
		this.dropDuplicates = dropDuplicates;
		
	}

	public void parserLoad( String arg)
	{
		final String queryDropTripleTable = String.format("DROP TABLE IF EXISTS %s", name_tripletable);
		final String queryDropTripleTableFixed = String.format("DROP TABLE IF EXISTS %s", name_tripletable);

		spark = SparkSession.builder().appName("Ntriples Parser").enableHiveSupport().getOrCreate();
		spark.sql(queryDropTripleTable);
		spark.sql(queryDropTripleTableFixed);


		String createTripleTableFixed = String.format(
				"CREATE TABLE  IF NOT EXISTS  %1$s(%2$s STRING, %3$s STRING, %4$s STRING, %5$s STRING) STORED AS PARQUET",
				name_tripletable, column_name_subject, column_name_predicate, column_name_object, column_name_object_type);

		parseDirectory(arg);
		
		

	}

	private void parseDirectory(String directory) {
		

		JavaSparkContext sparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
		MyParser par = new MyParser();
		boolean tableIsCreated = false;
		try {
			RemoteIterator<LocatedFileStatus> i = FileSystem.get(sparkContext.hadoopConfiguration()).listFiles(new Path(directory), false);
			
			while (i.hasNext()) {
				String createOrInsert;
				Path path = i.next().getPath();
				JavaRDD<String> inputFile = sparkContext.textFile(path.toString());
				JavaRDD<RDFStatement> parsedStatements = inputFile.map(line -> par.parseLine(line));

				Dataset<Row> dataset = spark.createDataFrame(parsedStatements, RDFStatement.class);
				dataset.createOrReplaceTempView("tempTable");
				if (!tableIsCreated) {
					tableIsCreated = true;
					createOrInsert = String.format(
							"CREATE TABLE  IF NOT EXISTS %1$s AS SELECT * FROM tempTable", name_tripletable);
				}
				else {
					createOrInsert = String.format(
							"INSERT INTO %1$s SELECT * FROM tempTable", name_tripletable);
				}
				spark.sql(createOrInsert);
			}

			if (!i.hasNext())
				return;
		} catch(IOException e) {

		}

	}


	@Override
	public void load() throws Exception {
		logger.info("PHASE 1: loading all triples to a generic table...");
		final String queryDropTripleTable = String.format("DROP TABLE IF EXISTS %s", name_tripletable);
		final String queryDropTripleTableFixed = String.format("DROP TABLE IF EXISTS %s", name_tripletable);
		String createTripleTableFixed = null;
		String repairTripleTableFixed = null;

		spark.sql(queryDropTripleTable);
		spark.sql(queryDropTripleTableFixed);

		logger.info(name_tripletable);
		// Main line of code that performs parsing
		parserLoad(hdfs_input_directory);



		spark.sql("show tables").show();

		final String queryAllTriples = String.format("SELECT * FROM %s", name_tripletable);
		Dataset<Row> allTriples = spark.sql(queryAllTriples);

		if (allTriples.count() == 0) {
			logger.error("Either your HDFS path does not contain any files or "
					+ "no triples were accepted in the given format (nt)");
			logger.error("The program will stop here.");
			throw new Exception("Empty HDFS directory or empty files within.");
		} else {
			logger.info("Total number of triples loaded: " + allTriples.count());
		}

		final List<Row> cleanedList = allTriples.limit(10).collectAsList();
		logger.info("First 10 cleaned triples (less if there are less): " + cleanedList);
	}


	
}
