package loader;

import java.io.File;
import java.io.FileNotFoundException;
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
 * Contains methods for loading and validating settings profiles for the loader module.
 */
public class Settings {
	private static final String DEFAULT_SETTINGS_FILE = "loader_default.ini";

	private String inputPath;
	private String settingsPath;
	private String databaseName;
	private boolean computeStatistics = false;
	private boolean dropDuplicateTriples = false;
	private boolean computeCharacteristicSets = false;
	private boolean generateTT = false;
	private boolean generateWPT = false;
	private boolean generateVP = false;
	private boolean generateIWPT = false;
	private boolean generateJWPT = false;
	// options for physical partitioning
	private boolean ttPartitionedByPredicate = false;
	private boolean ttPartitionedBySubject = false;
	private boolean wptPartitionedBySubject = false;
	private boolean vpPartitionedBySubject = false;

	public Settings(final String[] args) throws Exception {
		parseArguments(args);
		if (settingsPath == null) {
			settingsPath = DEFAULT_SETTINGS_FILE;
		}

		final File file = new File(settingsPath);
		if (file.exists()) {
			//noinspection MismatchedQueryAndUpdateOfCollection
			final Ini settings = new Ini(file);
			this.computeStatistics = settings.get("postprocessing", "computeStatistics", boolean.class);
			this.dropDuplicateTriples = settings.get("postprocessing", "dropDuplicates", boolean.class);
			this.computeCharacteristicSets = settings.get("postprocessing", "computeCharacteristicSets", boolean.class);

			this.generateTT = settings.get("logicalPartitioning", "TT", Boolean.class);
			this.generateWPT = settings.get("logicalPartitioning", "WPT", boolean.class);
			this.generateVP = settings.get("logicalPartitioning", "VP", boolean.class);
			this.generateIWPT = settings.get("logicalPartitioning", "IWPT", boolean.class);
			this.generateJWPT = settings.get("logicalPartitioning", "JWPT", boolean.class);

			this.ttPartitionedByPredicate = settings.get("physicalPartitioning", "ttp", boolean.class);
			this.ttPartitionedBySubject = settings.get("physicalPartitioning", "tts", boolean.class);
			this.wptPartitionedBySubject = settings.get("physicalPartitioning", "wpts", boolean.class);
			this.vpPartitionedBySubject = settings.get("physicalPartitioning", "vps", boolean.class);
		} else if (!settingsPath.equals(DEFAULT_SETTINGS_FILE)) {
			throw new FileNotFoundException();
		}
		validate();
		printLoggerInformation();
	}

	//TODO not everything is being tested
	private void validate() throws Exception {
		if (databaseName == null) {
			throw new Exception("Missing database name.");
		}

		if (!generateTT && !generateWPT && !generateVP && !generateIWPT && !generateJWPT) {
			throw new Exception("Invalid settings");
		}

		if (generateTT && inputPath == null) {
			throw new Exception("Cannot generate TT without the input path");
		}
	}

	//TODO allow old arguments to override settings from the initialization file
	private void parseArguments(final String[] args) {
		final CommandLineParser parser = new PosixParser();
		final Options options = new Options();

		final Option inputOption = new Option("i", "input", true, "HDFS input path of the RDF graph.");
		inputOption.setRequired(false);
		options.addOption(inputOption);

		final Option databaseOption = new Option("db", "database", true, "Output database name.");
		databaseOption.setRequired(true);
		options.addOption(databaseOption);

		final Option settingsPathOption = new Option("pref", "preferences", true, "Path to settings profile file.");
		settingsPathOption.setRequired(false);
		options.addOption(settingsPathOption);

		final Option helpOption = new Option("h", "help", false, "Print this help.");
		helpOption.setRequired(false);
		options.addOption(helpOption);


		final HelpFormatter formatter = new HelpFormatter();
		CommandLine cmd = null;
		try {
			cmd = parser.parse(options, args);
		} catch (final MissingOptionException e) {
			formatter.printHelp("JAR", "Load an RDF graph", options, "", true);
			return;
		} catch (final ParseException e) {
			e.printStackTrace();
		}

		assert cmd != null;
		if (cmd.hasOption("help")) {
			formatter.printHelp("JAR", "Load an RDF graph as Property Table using SparkSQL", options, "", true);
			return;
		}
		if (cmd.hasOption("input")) {
			inputPath = cmd.getOptionValue("input");
		}
		if (cmd.hasOption("database")) {
			databaseName = cmd.getOptionValue("database");
		}
		if (cmd.hasOption("preferences")) {
			settingsPath = cmd.getOptionValue("preferences");
		}
	}

	private void printLoggerInformation() {
		final Logger logger = Logger.getLogger("PRoST");

		logger.info("Using preference settings: " + settingsPath);
		logger.info("Output database set to: " + databaseName);
		logger.info("Input folder path set to: " + inputPath);

		final ArrayList<String> enabledLogicalPartitioningStrategies = new ArrayList<>();
		if (generateTT) {
			enabledLogicalPartitioningStrategies.add("TT");
		}
		if (generateVP) {
			enabledLogicalPartitioningStrategies.add("VP");
		}
		if (generateWPT) {
			enabledLogicalPartitioningStrategies.add("WPT");
		}
		if (generateIWPT) {
			enabledLogicalPartitioningStrategies.add("IWPT");
		}
		if (generateJWPT) {
			enabledLogicalPartitioningStrategies.add("JWPT");
		}
		logger.info("Logical Partitioning Strategies: " + String.join(", ", enabledLogicalPartitioningStrategies));

		final ArrayList<String> enabledPhysicalPartitioningStrategies = new ArrayList<>();
		if (ttPartitionedBySubject) {
			enabledPhysicalPartitioningStrategies.add("TT partitioned by subject");
		}
		if (ttPartitionedByPredicate) {
			enabledPhysicalPartitioningStrategies.add("TT partitioned by predicate");
		}
		if (wptPartitionedBySubject) {
			enabledPhysicalPartitioningStrategies.add("WPT partitioned by subject");
		}
		logger.info("Physical Partitioning Strategies: " + String.join(", ", enabledPhysicalPartitioningStrategies));

		final ArrayList<String> enabledPostProcessingOptions = new ArrayList<>();
		if (dropDuplicateTriples) {
			enabledPostProcessingOptions.add("Removing duplicate triples");
		}
		if (computeStatistics) {
			enabledPostProcessingOptions.add("Updating statistics file");
		}
		if (computeCharacteristicSets) {
			enabledPostProcessingOptions.add("Computing characteristic sets annotations");
		}
		logger.info("Post processing options: " + String.join(", ", enabledPostProcessingOptions));

	}

	public String getInputPath() {
		return inputPath;
	}

	public String getDatabaseName() {
		return databaseName;
	}

	public boolean isComputingStatistics() {
		return computeStatistics;
	}

	public boolean isDroppingDuplicateTriples() {
		return dropDuplicateTriples;
	}

	public boolean isComputingCharacteristicSets() {
		return computeCharacteristicSets;
	}

	public boolean isGeneratingTT() {
		return generateTT;
	}

	public boolean isGeneratingWPT() {
		return generateWPT;
	}

	public boolean isGeneratingVP() {
		return generateVP;
	}

	public boolean isGeneratingIWPT() {
		return generateIWPT;
	}

	public boolean isGeneratingJWPT() {
		return generateJWPT;
	}

	public boolean isTtPartitionedByPredicate() {
		return ttPartitionedByPredicate;
	}

	public boolean isTtPartitionedBySubject() {
		return ttPartitionedBySubject;
	}

	public boolean isWptPartitionedBySubject() {
		return wptPartitionedBySubject;
	}

	public boolean isVpPartitionedBySubject() {
		return vpPartitionedBySubject;
	}
}
