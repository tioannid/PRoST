package loader2;

import java.util.List;
import loader2.configuration.PredTbl;
import loader2.configuration.TripleTableSchema;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 *
 * @author tioannid
 */
public class VerticalPartitioningLoader extends TripleTableLoader {


    // ----- DATA MEMBERS -----
    private boolean generateExtVP;
    private String dictionaryTable;
    private List<PredTbl> predDictionary;
    private double threshold;

    // ----- CONSTRUCTORS -----
    public VerticalPartitioningLoader(SparkSession spark, String dbName,
            boolean flagDBExists, boolean flagCreateDB, String hdfsInputDir,
            boolean requiresInference, boolean ttPartitionedBySub,
            boolean ttPartitionedByPred, boolean dropDuplicates,
            TripleTableSchema tttschema, TripleTableSchema gttschema,
            String namespacePrefixJSONFile, boolean createUseNsDict,
            boolean useHiveQL_TableCreation, String asWKTFile,
            final String dictionaryTable, boolean generateExtVP,
            double thresholdExtVP) throws Exception {
        super(spark, dbName, flagDBExists, flagCreateDB, hdfsInputDir,
                requiresInference, ttPartitionedBySub, ttPartitionedByPred,
                dropDuplicates, tttschema, gttschema, namespacePrefixJSONFile,
                createUseNsDict, useHiveQL_TableCreation, asWKTFile);
        this.generateExtVP = generateExtVP;
        this.dictionaryTable = dictionaryTable;
        this.threshold = thresholdExtVP;
    }

    // ----- DATA ACCESSORS -----
    // ----- METHODS --------
    @Override
    public void load() throws Exception {
        super.load();
        logger.info("PHASE 2: creating VP tables on...");

        // 2.1 Create dictionary table for predicates with their statistics
        Dataset<Row> propsDS = spark.sql(
                String.format("SELECT %1$s, count(*), count(%2$s), count(%3$s) from %4$s group by %1$s",
                        this.tttschema.getColname_pred(),
                        this.tttschema.getColname_subj(),
                        this.tttschema.getColname_obj(),
                        this.tttschema.getTblname())).cache();
 
        JavaPairRDD<Row,Long> indxRDD = propsDS.javaRDD().zipWithIndex().cache();
        JavaRDD<PredTbl> propsRDD = indxRDD.map(t -> new PredTbl(t._1().getString(0), "prop" + t._2(), t._1().getLong(1), t._1().getLong(2), t._1().getLong(3))).cache();
//        StringBuilder sb = new StringBuilder();
//        for (PredTbl predtbl: propsRDD.collect()) {
//            sb.append(predtbl.toString());
//        }
//        logger.info(sb.toString());

        this.predDictionary = propsRDD.collect();
        Dataset<Row> predtblDS = spark.createDataFrame(propsRDD, PredTbl.class);
//        predtblDS.createOrReplaceTempView("tmp");
//        spark.sql("SELECT * FROM tmp").show();
        predtblDS.write().saveAsTable(dictionaryTable);
        
        // 2.2. Create all property tables
        String createVPTable;
        for (PredTbl predtbl : this.predDictionary) {
            createVPTable = String.format(
                "CREATE TABLE %1$s AS SELECT %2$s, %3$s FROM %4$s WHERE %5$s = '%6$s'", 
                    predtbl.getTblName(), tttschema.getColname_subj(),
                    tttschema.getColname_obj(), tttschema.getTblname(),
                    tttschema.getColname_pred(), predtbl.getPred());            
            spark.sql(createVPTable);
        }
    }
}
