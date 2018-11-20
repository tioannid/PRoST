package utils;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;

import org.apache.log4j.Logger;

import joinTree.ProtobufStats;

/**
 * This class is used to parse statistics from a Protobuf file and it exposes methods to
 * retrieve singular entries.
 *
 * TODO: implement whole graph statistics
 *
 * @author Matteo Cossu
 *
 */
public class Stats {

	private static final Logger logger = Logger.getLogger("PRoST");

	// single instance of the statistics
	private static Stats instance = null;

	private static boolean areStatsParsed = false;

	private HashMap<String, joinTree.ProtobufStats.TableStats> tableStats;
	private HashMap<String, Integer> tableSize;
	private HashMap<String, Integer> tableDistinctSubjects;
	private HashMap<String, Boolean> iptPropertyComplexity;
	private String[] tableNames;

	protected Stats() {
		// Exists only to defeat instantiation.
	}

	public static Stats getInstance() {
		if (instance == null) {
			instance = new Stats();
			instance.tableSize = new HashMap<>();
			instance.tableDistinctSubjects = new HashMap<>();
			instance.tableStats = new HashMap<>();
			instance.iptPropertyComplexity = new HashMap<>();
			return instance;
		}
		if (areStatsParsed) {
			return instance;
		} else {
			System.err.println("You should invoke parseStats before using the instance.");
			return null;
		}
	}

	public void parseStats(final String fileName) {
		if (areStatsParsed) {
			return;
		} else {
			areStatsParsed = true;
		}

		ProtobufStats.Graph graph;
		try {
			graph = ProtobufStats.Graph.parseFrom(new FileInputStream(fileName));
		} catch (final FileNotFoundException e) {
			logger.error("Statistics input File Not Found");
			return;
		} catch (final IOException e) {
			e.printStackTrace();
			return;
		}

		tableNames = new String[graph.getTablesCount()];
		int i = 0;
		for (final ProtobufStats.TableStats table : graph.getTablesList()) {
			tableNames[i] = table.getName();
			tableStats.put(tableNames[i], table);
			tableSize.put(tableNames[i], table.getSize());
			tableDistinctSubjects.put(tableNames[i], table.getDistinctSubjects());
			iptPropertyComplexity.put(tableNames[i], table.getIsInverseComplex());
			i++;
		}
		logger.info("Statistics correctly parsed");
	}

	public int getTableSize(String table) {
		table = findTableName(table);
		if (table == null) {
			return -1;
		}
		return tableSize.get(table);
	}

	public int getTableDistinctSubjects(String table) {
		table = findTableName(table);
		if (table == null) {
			return -1;
		}
		return tableDistinctSubjects.get(table);
	}

	public ProtobufStats.TableStats getTableStats(String table) {
		table = findTableName(table);
		if (table == null) {
			return null;
		}
		return tableStats.get(table);
	}

	public boolean isTableComplex(final String table) {
		final String cleanedTableName = findTableName(table);
		return getTableSize(cleanedTableName) != getTableDistinctSubjects(cleanedTableName);
	}

	public boolean isInverseTableComplex(String table) {
		table = findTableName(table);
		return iptPropertyComplexity.get(table);
	}

	/*
	 * This method returns the same name for the table (VP) or column (PT) that was used in
	 * the loading phase. Returns the name from an exact match or from a partial one, if a
	 * prefix was used in loading or in the query. Return null if there is no match
	 */
	public String findTableName(final String tableName) {
		String cleanedTableName = Utils.toMetastoreName(tableName).toLowerCase();
		for (final String realTableName : tableNames) {

			final boolean exactMatch = realTableName.equalsIgnoreCase(cleanedTableName);
			// if there is a match, return the correct table name
			if (exactMatch) {
				return realTableName;
			}
		}
		// not found
		return null;
	}
}