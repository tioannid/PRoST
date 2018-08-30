package extVp;

import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import org.apache.log4j.Logger;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.shared.PrefixMapping;

/**
 * Class containing usage statistics of an ExtVP table
 *
 */
public class TableStatistic implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = -4633496814794267474L;
	private static final Logger logger = Logger.getLogger("PRoST");
	
	private String tableName;
	private float selectivity;
	private long size; //number of rows
	private Boolean tableExists; //True if table currently exists in the database
	private HashSet<LiteralsTuple> containedLiterals;
	
	public TableStatistic (String tableName, float selectivity, long size) {
		this.tableName = tableName;
		this.selectivity = selectivity;
		this.size = size;
		this.tableExists = true;
	}
	
	public TableStatistic (String tableName, float selectivity, long size, HashSet<LiteralsTuple> containedLiterals) {
		this.tableName = tableName;
		this.selectivity = selectivity;
		this.size = size;
		this.tableExists = true;
		this.containedLiterals = containedLiterals;
	}
	
	public String getTableName() {
		return tableName;
	}
	
	public float getSelectivity() {
		return selectivity;
	}
	
	public long getSize() {
		return size;
	}
	
	public Boolean getTableExists() {
		return tableExists;
	}
	
	public void setTableExists(Boolean tableExists) {
		this.tableExists = tableExists;
	}
	
	public Boolean containLiteralsTuple(String outerLiteral, String innerLiteral) {
		LiteralsTuple literalsTuple = new LiteralsTuple(outerLiteral, innerLiteral);
		return containedLiterals.contains(literalsTuple);
	}
	
	public void insertLiteralsTuple(String outerLiteral, String innerLiteral) {
		containedLiterals.add(new LiteralsTuple(outerLiteral, innerLiteral));
	}
	
	/**
	 * Find an ExtVP table to be used for a triple
	 * 
	 * <p>Given a triple, and a list of all triples in the query, returns the best existing ExtVP table 
	 * that exists in the database that can be used for that triple</p>
	 * 
	 * @param currentTriple Triple for which a valid ExtVP table must be searched for
	 * @param triples List of all triples in the query
	 * @param statistics HashMap with all table statistics of the database being used
	 * @param prefixes PrefixMapping of the triples
	 * @return Name of table to be used. Returns and empty string if no table was selected.
	 */
	public static String selectExtVPTable(Triple currentTriple, List<Triple> triples, Map<String, TableStatistic> statistics, 
			PrefixMapping prefixes) {
		String currentSubject = currentTriple.getSubject().toString(prefixes);
		String currentObject = currentTriple.getObject().toString(prefixes);
		String currentPredicate = currentTriple.getPredicate().toString(prefixes);
		
		String selectedTableName = "";
		float currentTableScore = 1; //score of 1 means that the extVP table is equal to the VP table
		
		String unidexedSelectedTableName = "";
		float unindexeCurrentdTableScore = 1; //score of 1 means that the extVP table is equal to the VP table
				
		ListIterator<Triple> triplesIterator = triples.listIterator();
		while (triplesIterator.hasNext()) {
			Triple outerTriple = triplesIterator.next();
			if (outerTriple!=currentTriple) {
				String outerSubject = outerTriple.getSubject().toString(prefixes);
				String outerObject = outerTriple.getObject().toString(prefixes);
				String outerPredicate = outerTriple.getPredicate().toString(prefixes);
				if (currentTriple.getSubject().isVariable() && outerTriple.getSubject().isVariable() && currentSubject.equals(outerSubject)) {
					//SS
					String tableName = ExtVpCreator.getExtVPTableName(currentPredicate, outerPredicate, ExtVpCreator.extVPType.SS);
					TableStatistic tableStatistic = statistics.get(tableName);
					if (tableStatistic!=null) {
						if (tableStatistic.getTableExists()==true && tableStatistic.getSelectivity()<currentTableScore) {
							selectedTableName = tableName;
							currentTableScore = tableStatistic.getSelectivity();
						} else if (tableStatistic.getTableExists()==false && tableStatistic.getSelectivity()<unindexeCurrentdTableScore) {
							unidexedSelectedTableName = tableName;
							unindexeCurrentdTableScore = tableStatistic.getSelectivity();
						}
					}
				}
		
				if (currentTriple.getObject().isVariable() && outerTriple.getObject().isVariable() && currentObject.equals(outerObject)) {
					//OO	
					String tableName = ExtVpCreator.getExtVPTableName(currentPredicate, outerPredicate, ExtVpCreator.extVPType.OO);
					TableStatistic tableStatistic = statistics.get(tableName);
					if (tableStatistic!=null) {
						if (tableStatistic.getTableExists()==true && tableStatistic.getSelectivity()<currentTableScore) {
							selectedTableName = tableName;
							currentTableScore = tableStatistic.getSelectivity();
						} else if (tableStatistic.getTableExists()==false && tableStatistic.getSelectivity()<unindexeCurrentdTableScore) {
							unidexedSelectedTableName = tableName;
							unindexeCurrentdTableScore = tableStatistic.getSelectivity();
						}
					}
				}
				if (currentTriple.getSubject().isVariable() && outerTriple.getObject().isVariable() && currentSubject.equals(outerObject)) {
					//SO
					String tableName = ExtVpCreator.getExtVPTableName(currentPredicate, outerPredicate, ExtVpCreator.extVPType.SO);
					TableStatistic tableStatistic = statistics.get(tableName);
					if (tableStatistic!=null) {
						if (tableStatistic.getTableExists()==true && tableStatistic.getSelectivity()<currentTableScore) {
							selectedTableName = tableName;
							currentTableScore = tableStatistic.getSelectivity();
						} else if (tableStatistic.getTableExists()==false && tableStatistic.getSelectivity()<unindexeCurrentdTableScore) {
							unidexedSelectedTableName = tableName;
							unindexeCurrentdTableScore = tableStatistic.getSelectivity();
						}
					}
				}
				if (currentTriple.getObject().isVariable() && outerTriple.getSubject().isVariable() &&  currentObject.equals(outerSubject)) {
					//OS	
					String tableName = ExtVpCreator.getExtVPTableName(currentPredicate, outerPredicate, ExtVpCreator.extVPType.OS);
					TableStatistic tableStatistic = statistics.get(tableName);
					if (tableStatistic!=null) {
						if (tableStatistic.getTableExists()==true && tableStatistic.getSelectivity()<currentTableScore) {
							selectedTableName = tableName;
							currentTableScore = tableStatistic.getSelectivity();
						} else if (tableStatistic.getTableExists()==false && tableStatistic.getSelectivity()<unindexeCurrentdTableScore) {
							unidexedSelectedTableName = tableName;
							unindexeCurrentdTableScore = tableStatistic.getSelectivity();
						}
					}
				}
			}
			
			//TODO If there's no existing table in the cache, create the table, if there is an unindexed table with good selectivity.
		}
		
		if (selectedTableName != "") {
			logger.info("selected table selectivity: " + currentTableScore + ", table name: " + selectedTableName);
		}
		if (unidexedSelectedTableName != "") {
			logger.info("best unindexed table selectivity: " + unindexeCurrentdTableScore + ", table name: " + unidexedSelectedTableName);
		}
		
		return selectedTableName;
	}
}
