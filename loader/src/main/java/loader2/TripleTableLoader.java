package loader2;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import loader2.configuration.TripleTableSchema;
import loader2.utils.Namespace;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import loader2.utils.NamespaceDictionary;
import org.apache.spark.api.java.function.Function;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.eclipse.rdf4j.model.vocabulary.GEO;
import org.eclipse.rdf4j.model.vocabulary.RDFS;

/**
 * Class that constructs a triples table. First, the loader creates an external
 * table ("raw"). The data is read using SerDe capabilities and by means of a
 * regular expression. An additional table ("fixed") is created to make sure
 * that only valid triples are passed to the next stages in which other models
 * e.g. Property Table, or Vertical Partitioning are built.
 *
 * @author Matteo Cossu
 * @author Victor Anthony Arrascue Ayala
 */
public class TripleTableLoader extends Loader implements Serializable {

    // STATIC MEMBERS
    protected static String nsPrefTableName = "nsprefixes";

    // STATIC DATA ACCESSORS
    public static String getNsPrefTableName() {
        return nsPrefTableName;
    }

    // The use of this method is ONLY for setting a different name for
    // the table to store the namespace prefixes BEFORE creating a
    // new TripleTableLoader object. 
    // TODO: It can be used to trigger the change of namespace prefixes table
    public static void setNsPrefTableName(String nsPrefTableName) {
        TripleTableLoader.nsPrefTableName = nsPrefTableName;
    }

    // DATA MEMBERS    
    protected boolean ttPartitionedBySub = false;
    protected boolean ttPartitionedByPred = false;
    protected boolean dropDuplicates = true;
    // Schema of target table for thematic triples
    protected TripleTableSchema tttschema = null;
    // Schema of target table for geostatial triples
    protected TripleTableSchema gttschema = null;
    // protected final Broadcast<NamespaceDictionary> nsDict;
    protected boolean createUseNsDict = true;
    protected String namespacePrefixJSONFile = "";
    protected NamespaceDictionary nsDict = null;

    // CONSTRUCTORS
    // 1. Detailed/Base Constructor
    public TripleTableLoader(final SparkSession spark, final String dbName,
            boolean flagDBExists, boolean flagCreateDB,
            final String hdfsInputDir,
            boolean requiresInference,
            final boolean ttPartitionedBySub, final boolean ttPartitionedByPred,
            final boolean dropDuplicates,
            TripleTableSchema tttschema, TripleTableSchema gttschema,
            String namespacePrefixJSONFile, boolean createUseNsDict,
            final boolean useHiveQL_TableCreation) throws Exception {
        super(spark, dbName, flagDBExists, flagCreateDB, hdfsInputDir, useHiveQL_TableCreation);
        this.ttPartitionedBySub = ttPartitionedBySub;
        this.ttPartitionedByPred = ttPartitionedByPred;
        this.dropDuplicates = dropDuplicates;
        this.tttschema = tttschema;
        this.gttschema = gttschema;
        this.createUseNsDict = createUseNsDict;
        this.namespacePrefixJSONFile = namespacePrefixJSONFile;
        if (createUseNsDict) {
            nsDict = new NamespaceDictionary(spark, namespacePrefixJSONFile, nsPrefTableName);
        } else {
            nsDict = null;
        }
    }

    // DATA ACCESSORS
    public boolean isTtPartitionedBySub() {
        return ttPartitionedBySub;
    }

    public void setTtPartitionedBySub(boolean ttPartitionedBySub) {
        this.ttPartitionedBySub = ttPartitionedBySub;
    }

    public boolean isTtPartitionedByPred() {
        return ttPartitionedByPred;
    }

    public void setTtPartitionedByPred(boolean ttPartitionedByPred) {
        this.ttPartitionedByPred = ttPartitionedByPred;
    }

    public boolean isDropDuplicates() {
        return dropDuplicates;
    }

    public void setDropDuplicates(boolean dropDuplicates) {
        this.dropDuplicates = dropDuplicates;
    }

    public TripleTableSchema getTttschema() {
        return tttschema;
    }

    public void setTttschema(TripleTableSchema tttschema) {
        this.tttschema = tttschema;
    }

    public TripleTableSchema getGttschema() {
        return gttschema;
    }

    public void setGttschema(TripleTableSchema gttschema) {
        this.gttschema = gttschema;
    }

    public boolean isCreateUseNsDict() {
        return createUseNsDict;
    }

    public void setCreateUseNsDict(boolean createUseNsDict) {
        this.createUseNsDict = createUseNsDict;
    }

    public String getNamespacePrefixJSONFile() {
        return namespacePrefixJSONFile;
    }

    public void setNamespacePrefixJSONFile(String namespacePrefixJSONFile) {
        this.namespacePrefixJSONFile = namespacePrefixJSONFile;
    }

    public NamespaceDictionary getNsDict() {
        return nsDict;
    }

    public void setNsDict(NamespaceDictionary nsDict) {
        this.nsDict = nsDict;
    }

    // Replace all occurences of ns.uri in rdf statement with ns.namespace
    // MAX 3 replacements are allowed per triple, which is the logical thing to expect!
    static class EncodeRDF implements Function<RDFStatement, RDFStatement> {

        private final List<Namespace> nsList;

        public EncodeRDF(List<Namespace> nsList) {
            this.nsList = nsList;
        }

        @Override
        public RDFStatement call(RDFStatement rdf) {
            Namespace ns;
            int cntReplaced = 0, listlen = nsList.size(), i;
            String serializedRDF = rdf.serialize(), serializedRDF_;
            // 
            for (i = 0; (i < listlen && (cntReplaced != 3)); i++) {
                ns = nsList.get(i);
                serializedRDF_ = serializedRDF.replaceAll(ns.getUri(), ns.getNamespace());
                if (serializedRDF_.compareTo(serializedRDF) != 0) {
                    cntReplaced++;
                }
                serializedRDF = serializedRDF_;
            }
            return RDFStatement.deserialize(serializedRDF, rdf.getoType());
        }
    }

    // METHODS
    private void parseDirectory(String directory) {
        JavaSparkContext sparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
        //MyParser par = new MyParser();
        ModularParser par = new ModularParser();
        try {
            RemoteIterator<LocatedFileStatus> i = FileSystem.get(sparkContext.hadoopConfiguration()).listFiles(new Path(directory), false);
            while (i.hasNext()) {
                Path path = i.next().getPath();
                JavaRDD<String> inputFile = sparkContext.textFile(path.toString());
                JavaRDD<RDFStatement> parsedStatements = inputFile.map(line -> par.parseLine(line));

                /* Find the triples (s,p,o) that will feed the spatial index
                    1) p == GEO.AS_WKT  ==> (p is the geospatial property) OR
               
                    2) o == GEO.AS_WKT AND p == RDFS.SUBPROPERTYOF ==> (s is the geospatial property)
                
                        sql("select s,p,o from triples where o like 'geo:asWKT'").show(false);
                
                JavaRDD<RDFStatement> asWKTRDD = parsedStatements.filter(rdf -> new Function<RDFStatement, Boolean>() {
                    @Override
                    public Boolean call(RDFStatement rdf) throws Exception {
                        return new Boolean(((rdf.getP() == GEO.AS_WKT.toString()) || (rdf.getP() == RDFS.SUBPROPERTYOF.toString() && rdf.getO() == GEO.AS_WKT.toString())));
                    }
                
                })
                 */
                if (createUseNsDict) {
                    parsedStatements = parsedStatements.map(new EncodeRDF(nsDict.getNsList()));
                }
                Dataset<Row> dictEncodedRdfDS = spark.createDataFrame(parsedStatements, RDFStatement.class);
                if (this.useHiveQL_TableCreation) { // use HiveQL
                    dictEncodedRdfDS.createOrReplaceTempView("tmp_dictencoded");
                    spark.sql(String.format(
                            "CREATE TABLE %1$s AS SELECT * FROM tmp_dictencoded",
                            tttschema.getTblname()));
                } else {    // use Spark SQL
                    dictEncodedRdfDS.write().saveAsTable(tttschema.getTblname());
                }
            }
        } catch (IOException e) {
        }
    }

    @Override
    public void load() throws Exception {
        logger.info("PHASE 1: loading all triples to a generic table...");
        spark.sql(String.format("DROP TABLE IF EXISTS %s", tttschema.getTblname()));
//        logger.info(queryDropTripleTable);

        logger.info("Dropped " + tttschema.getTblname() + " table and recreating it");
        // Main line of code that performs parsing
        parseDirectory(hdfsInputDir);

//        spark.sql("show tables").show();
        final String queryAllTriples = String.format("SELECT * FROM %s", tttschema.getTblname());
        Dataset<Row> allTriples = spark.sql(queryAllTriples);
//        logger.info(queryAllTriples);

        long noOfTriples = allTriples.count();
        if (noOfTriples == 0) {
            logger.error("Either your HDFS path does not contain any files or "
                    + "no triples were accepted in the given format (nt)");
            logger.error("The program will stop here.");
            throw new Exception("Empty HDFS directory or empty files within.");
        } else {
            logger.info("Total number of triples loaded: " + noOfTriples);
        }

        final List<Row> cleanedList = allTriples.limit(10).collectAsList();
        logger.info("First 10 cleaned triples (less if there are less): "
                + ((nsDict != null) ? this.nsDict.getTurtlePrefixHeader() : "") + "\n" + cleanedList);
    }

}
