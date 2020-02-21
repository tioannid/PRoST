package loader;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

public class ExtVPCreator {

	private Map<String, String> predDictionary;
	private SparkSession spark;
	private double threshold;
	private Map<String, TableInfo> statistics;
	private int savedTables;
	private int unsavedTables;
	private StringBuffer extVPStats;
	private List<String> tablesWithIRIs;
	protected static final Logger logger = Logger.getLogger("PRoST");

	public ExtVPCreator(Map<String, String> predDictionary, SparkSession spark, double threshold,
			Map<String, TableInfo> statistics, List<String> tablesWithIRIs2) {
		this.predDictionary = predDictionary;
		this.spark = spark;
		this.threshold = threshold;
		this.statistics = statistics;
		savedTables = 0;
		unsavedTables = 0;
		extVPStats = new StringBuffer();
		this.tablesWithIRIs=tablesWithIRIs2;
	}

	public void createExtVP(String relType) {
		int savedTables = 0;
		int unsavedNonEmptyTables = 0;
		// var createdDirs = List[String]()

		// for every VP table generate a set of ExtVP tables, which represent its
		// (relType)-relations to the other VP tables
		for (String propIri : predDictionary.keySet()) {
			String pred1=predDictionary.get(propIri);
			if(relType == "OS" && (!tablesWithIRIs.contains(pred1)||  propIri.equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"))) {
				//objects in pred1 are literals or rdf:type values, nothing to do
				//since Os is the last call after SS and SO we will not need this table anymore
				spark.catalog().uncacheTable(pred1);
				logger.info("skipping OS extVP for property: "+propIri);
				continue;
			}
			// get all predicates, whose TPs are in (relType)-relation with TP
			// (?x, pred1, ?y)
			//List<String> relatedPredicates = getRelatedPredicates(pred1, relType);

			//for (String pred2iri : relatedPredicates) {
				for (String pred2iri : predDictionary.keySet()) {
					String pred2=predDictionary.get(pred2iri);
				
				
				if(pred2iri.equals("http://www.opengis.net/ont/geosparql#hasGeometry") ||
						pred2iri.equals("http://www.opengis.net/ont/geosparql#asWKT")) {
					//what to do?
					continue;
				}
				
				//String pred2=predDictionary.get(pred2iri);
				if(relType == "SO" && (!tablesWithIRIs.contains(pred2) || pred2iri.equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"))) {
					//objects in pred2 are literals or rdf:type values, nothing to do
					logger.info("skipping SO extVP for property: "+propIri);
					continue;
				}
				long extVpTableSize = -1L;

				// we avoid generation of ExtVP tables corresponding to subject-subject
				// relation to it self, since such tables are always equal to the
				// corresponding VP tables
				if (!(relType == "SS" && pred1 == pred2)) {
					spark.sql("DROP TABLE IF EXISTS "+relType + pred1 + pred2);
					String sqlCommand = getExtVpSQLcommand(pred1, pred2, relType);
					Dataset<Row> extVpTable = spark.sql(sqlCommand);
					extVpTable.createOrReplaceTempView(relType + pred1 + pred2);
					spark.catalog().cacheTable(relType + pred1 + pred2);
					//final Dataset<Row> table_VP = spark.sql("select * from "+property);
					//table_VP.persist(StorageLevel.MEMORY_AND_DISK());
					
					//extVpTable.registerTempTable("extvp_table");
					// cache table to avoid recomputation of DF by storage to HDFS
					//extVpTable.persist(StorageLevel.MEMORY_AND_DISK());
					//extVpTable.cache();
					// spark.catalog.cacheTable("extvp_table");
					extVpTableSize = extVpTable.count();

					// save ExtVP table in case if its size smaller than
					// ScaleUB*size(corresponding VPTable)
					if (extVpTableSize>0 && extVpTableSize < (statistics.get(pred1).getCountAll() * threshold)) {
						logger.info("saving extVP table for: "  + relType + pred1 + pred2 
								+ " with size :"+ extVpTableSize + "rows and original pred1 size: " +
								statistics.get(pred1).getCountAll() );
						extVpTable.write().saveAsTable(relType + pred1 + pred2);

						savedTables++;
					} else {
						unsavedTables++;
					}

					//extVpTable.unpersist();
					spark.catalog().uncacheTable(relType + pred1 + pred2);

				} else {
					extVpTableSize = statistics.get(pred1).getCountAll();
				}

				// print statistic line
				// save statistics about all ExtVP tables > 0, even about those, which
				// > then ScaleUB.
				// We need statistics about all non-empty tables
				// for the Empty Table Optimization (avoiding query execution for
				// the queries having triple pattern relations, which lead to empty
				// result)
				extVPStats.append(pred1);
				extVPStats.append(",");
				extVPStats.append(pred2);
				extVPStats.append(",");
				extVPStats.append(relType);
				extVPStats.append(",");
				extVPStats.append(extVpTableSize);
				extVPStats.append("\n");
				// StatisticWriter.addTableStatistic("<" + pred1 + "><" + pred2 + ">",
				// extVpTableSize,
				// _vpTableSizes(pred1))
			}
				//spark.catalog().uncacheTable(pred1);
		}

		// StatisticWriter.closeStatisticFile()

	}

	private List<String> getRelatedPredicates(String pred, String relType) {
		String sqlRelPreds = ("select distinct p " + "from triples t1 " + "left semi join " + pred + " t2 " + "on");

		if (relType == "SS") {
			sqlRelPreds += "(t1.s=t2.s)";
		} else if (relType == "OS") {
			sqlRelPreds += "(t1.s=t2.o)";
		} else if (relType == "SO") {
			sqlRelPreds += "(t1.p=t2.s)";
		}

		return spark.sql(sqlRelPreds).as(Encoders.STRING()).collectAsList();
		// .map(line[0], ).collectAsList().get(0)..map(t => t(0).toString()).collect();
	}

	private String getExtVpSQLcommand(String pred1, String pred2, String relType) {
		String command = ("select t1.s as s, t1.o as o " + "from " + pred1 + " t1 " + "left semi join " + pred2 + " t2 "
				+ "on ");

		if (relType == "SS") {
			command += "(t1.s=t2.s)";
		} else if (relType == "OS") {
			command += "(t1.o=t2.s)";
		} else if (relType == "SO") {
			command += "(t1.s=t2.o)";
		}

		return command;
	}

	public StringBuffer getExtVPStats() {
		return extVPStats;
	}
	
	

}
