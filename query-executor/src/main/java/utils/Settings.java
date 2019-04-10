package utils;

import java.io.File;
import java.util.ArrayList;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.log4j.Logger;
import org.ini4j.Ini;

/**
 * Loads and validates initialization options for the query-executor component.
 */
public class Settings {
	private static final String DEFAULT_SETTINGS_FILE = "query-executor_default.ini";

	//General settings
	private String settingsPath;
	private String databaseName;
	private String inputPath;
	private String statsPath;
	private String outputFilePath;
	private String benchmarkFilePath;

	// Node types
	private boolean usingTT = false;
	private boolean usingVP = false;
	private boolean usingWPT = false;
	private boolean usingIWPT = false;
	private boolean usingJWPT = false;
	private boolean usingEmergentSchema = false;
	private String emergentSchemaPath;

	// Translator options
	private int joinTreeMaximumWidth = -1;
	private boolean groupingTriples = true;
	private int minGroupSize = 2;

	// Executor options
	private boolean randomQueryOrder = false;
	private boolean savingBenchmarkFile = false;

	public Settings(final String[] args) throws Exception {
		parseArguments(args);
		if (settingsPath == null) {
			settingsPath = DEFAULT_SETTINGS_FILE;
		}
		if (statsPath == null) {
			statsPath = databaseName + ".stats";
		}

		final File file = new File(settingsPath);
		if (file.exists()) {
			//noinspection MismatchedQueryAndUpdateOfCollection
			final Ini settings = new Ini(file);
			this.usingTT = settings.get("nodeTypes", "TT", boolean.class);
			this.usingVP = settings.get("nodeTypes", "VP", boolean.class);
			this.usingWPT = settings.get("nodeTypes", "WPT", boolean.class);
			this.usingIWPT = settings.get("nodeTypes", "IWPT", boolean.class);
			this.usingJWPT = settings.get("nodeTypes", "JWPT", boolean.class);

			this.groupingTriples = settings.get("translator", "groupingTriples", boolean.class);
			this.minGroupSize = settings.get("translator", "minGroupSize", int.class);
			this.joinTreeMaximumWidth = settings.get("translator", "maxWidth", int.class);

			this.randomQueryOrder = settings.get("executor", "randomQueryOrder", boolean.class);
			this.savingBenchmarkFile = settings.get("executor", "savingBenchmarkFile", boolean.class);
		}

		if (savingBenchmarkFile && benchmarkFilePath == null) {
			benchmarkFilePath = createCsvFilename();
		}

		validate();
		printLoggerInformation();
	}

	private void validate() throws Exception {
		if (databaseName == null) {
			throw new Exception("Missing database name.");
		}

		if (!usingTT && !usingVP && !usingWPT && !usingIWPT && !usingJWPT) {
			throw new Exception("At least one node type must be enabled");
		}
	}

	private void parseArguments(final String[] args) {
		final CommandLineParser parser = new PosixParser();
		final Options options = new Options();

		final Option databaseOpt = new Option("db", "DB", true, "Database containing the Vertically Partitioned and "
				+ "Property Tables.");
		databaseOpt.setRequired(true);
		options.addOption(databaseOpt);

		final Option inputOpt = new Option("i", "input", true, "Input file/folder with the SPARQL query/queries.");
		inputOpt.setRequired(true);
		options.addOption(inputOpt);

		final Option settingsPathOption = new Option("pref", "preferences", true, "[OPTIONAL] Path to settings "
				+ "profile file. The default initialization file is used by default.");
		settingsPathOption.setRequired(false);
		options.addOption(settingsPathOption);

		final Option outputOpt = new Option("o", "output", true, "[OPTIONAL] Output path for the results in HDFS.");
		options.addOption(outputOpt);

		final Option statOpt = new Option("s", "stats", true, "[OPTIONAL] File with statistics. <databaseName>.stats "
				+ "will be used if no value is provided");
		options.addOption(statOpt);

		final Option emSchemaOpt = new Option("es", "emergentSchema", true, "[OPTIONAL] File with emergent schema, if "
				+ "exists");
		options.addOption(emSchemaOpt);

		final Option csvPathOpt = new Option("bench", "benchmark", true, "[OPTIONAL] Filename to write execution "
				+ "times. ");
		options.addOption(csvPathOpt);

		final Option helpOpt = new Option("h", "help", true, "Print this help.");
		options.addOption(helpOpt);

		final HelpFormatter formatter = new HelpFormatter();
		CommandLine cmd = null;
		try {
			cmd = parser.parse(options, args);
		} catch (final MissingOptionException e) {
			formatter.printHelp("JAR", "Execute a  SPARQL query with Spark", options, "", true);
			return;
		} catch (final ParseException e) {
			e.printStackTrace();
		}

		assert cmd != null;
		if (cmd.hasOption("help")) {
			formatter.printHelp("JAR", "Execute a  SPARQL query with Spark", options, "", true);
			return;
		}
		if (cmd.hasOption("DB")) {
			databaseName = cmd.getOptionValue("DB");
		}
		if (cmd.hasOption("input")) {
			inputPath = cmd.getOptionValue("input");
		}
		if (cmd.hasOption("preferences")) {
			settingsPath = cmd.getOptionValue("preferences");
		}
		if (cmd.hasOption("output")) {
			outputFilePath = cmd.getOptionValue("output");
		}
		if (cmd.hasOption("stats")) {
			statsPath = cmd.getOptionValue("stats");
		}
		if (cmd.hasOption("emergentSchema")) {
			emergentSchemaPath = cmd.getOptionValue("emergentSchema");
			usingEmergentSchema = true;
		}
		if (cmd.hasOption("benchmark")) {
			benchmarkFilePath = cmd.getOptionValue("benchmark");
			savingBenchmarkFile = true;
		}
	}

	private void printLoggerInformation() {
		final Logger logger = Logger.getLogger("PRoST");

		logger.info("Using preference settings: " + settingsPath);
		logger.info("Database set to: " + databaseName);
		logger.info("Input queries path set to: " + inputPath);
		if (outputFilePath != null) {
			logger.info("Output path set to: " + outputFilePath);
		}
		if (statsPath != null) {
			logger.info("Statistics file path set to: " + statsPath);
		}

		final ArrayList<String> enabledNodeTypes = new ArrayList<>();
		if (usingTT) {
			enabledNodeTypes.add("TT");
		}
		if (usingVP) {
			enabledNodeTypes.add("VP");
		}
		if (usingWPT) {
			enabledNodeTypes.add("WPT");
		}
		if (usingIWPT) {
			enabledNodeTypes.add("IWPT");
		}
		if (usingJWPT) {
			enabledNodeTypes.add("JWPT");
		}
		logger.info("Enabled node types: " + String.join(", ", enabledNodeTypes));

		if (usingEmergentSchema) {
			logger.info("Using emergent schema. Path: " + emergentSchemaPath);
		}

		logger.info("#TRANSLATOR OPTIONS#");
		if (joinTreeMaximumWidth == -1) {
			logger.info("Maximum width: heuristically computed.");
		} else {
			logger.info("Maximum width: " + joinTreeMaximumWidth);
		}
		if (groupingTriples) {
			logger.info("Grouping of triples enabled");
		} else {
			logger.info("Grouping of triples disabled");
		}
		logger.info("Minimum group size: " + minGroupSize);

		logger.info("#EXECUTOR OPTIONS#");
		if (randomQueryOrder) {
			logger.info("Queries execution order: random");
		} else {
			logger.info("Queries execution order: static");
		}
		if (savingBenchmarkFile) {
			logger.info("Saving benchmark file to: " + benchmarkFilePath);
		}
	}

	private String createCsvFilename() {
		final ArrayList<String> csvFilenameElements = new ArrayList<>();
		csvFilenameElements.add("times");
		if (usingTT) {
			csvFilenameElements.add("TT");
		}
		if (usingVP) {
			csvFilenameElements.add("VP");
		}
		if (usingWPT) {
			csvFilenameElements.add("WPT");
		}
		if (usingIWPT) {
			csvFilenameElements.add("IWPT");
		}
		if (usingJWPT) {
			csvFilenameElements.add("JWPT");
		}
		if (usingEmergentSchema) {
			csvFilenameElements.add("EmergentSchema");
		}
		if (joinTreeMaximumWidth != -1) {
			csvFilenameElements.add("maxWidth-" + joinTreeMaximumWidth);
		}
		if (!groupingTriples) {
			csvFilenameElements.add("nonGrouped");
		}
		csvFilenameElements.add("minGroup-" + minGroupSize);

		if (randomQueryOrder) {
			csvFilenameElements.add("randomOrder");
		}
		return String.join("_", csvFilenameElements) + ".csv";
	}

	public String getDatabaseName() {
		return databaseName;
	}

	public String getInputPath() {
		return inputPath;
	}

	public String getStatsPath() {
		return statsPath;
	}

	public String getOutputFilePath() {
		return outputFilePath;
	}

	public String getBenchmarkFilePath() {
		return benchmarkFilePath;
	}

	public boolean isUsingTT() {
		return usingTT;
	}

	public boolean isUsingVP() {
		return usingVP;
	}

	public boolean isUsingWPT() {
		return usingWPT;
	}

	public boolean isUsingIWPT() {
		return usingIWPT;
	}

	public boolean isUsingJWPT() {
		return usingJWPT;
	}

	public boolean isUsingEmergentSchema() {
		return usingEmergentSchema;
	}

	public String getEmergentSchemaPath() {
		return emergentSchemaPath;
	}

	public int getJoinTreeMaximumWidth() {
		return joinTreeMaximumWidth;
	}

	public boolean isGroupingTriples() {
		return groupingTriples;
	}

	public int getMinGroupSize() {
		return minGroupSize;
	}

	public boolean isRandomQueryOrder() {
		return randomQueryOrder;
	}

	public boolean isSavingBenchmarkFile() {
		return savingBenchmarkFile;
	}
}