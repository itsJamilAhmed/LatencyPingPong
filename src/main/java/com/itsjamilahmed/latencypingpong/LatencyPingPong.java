/**
 * 
 */
package com.itsjamilahmed.latencypingpong;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.ArgumentGroup;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;

/**
 * @author Jamil.Ahmed@Solace.com
 *
 */
public class LatencyPingPong {
	
	final static String programJarName = "LatencyPingPong.jar";
	final static String programVersion = "1.0.7";

	
	// If the publish and subscribe destinations for the ping/pong messages are not specified, how to form the default structure?
	final static String allTopicsPrefix = "LatencyPingPong";
	final static String pingTopicPrefix = allTopicsPrefix + "/Pings";
	final static String pongTopicPrefix = allTopicsPrefix + "/Pongs";
	final static String resultsTopicPrefix = allTopicsPrefix + "/Results";
	final static String summaryTopicPrefix = allTopicsPrefix + "/Summary";
	
	// A log4j logger to handle all stdout/log output 
	static 	Logger logger = Logger.getLogger(LatencyPingPong.class);
	
	static {
		System.setProperty("org.apache.commons.logging.Log",
				"org.apache.commons.logging.impl.NoOpLog");
	}
	/**
	 * @param args
	 */

	public static void main(String[] args) {
		
		
		// Print program details and DEMO-ONLY status...
		printStartBanner();
		
		// First use an argument parser library to handle all the command line flags and parameters
		Map<String,Object> parameters = parseArgs(args);
		
		// Now use the provided args to setup the logging properties
		setupLoggingProperties(parameters);
		logger.info("LatencyPingPong Program started. This is node name: " + parameters.get("node_name").toString() + " in node group: " + parameters.get("node_group"));
		logger.debug("Program started with arguments: " + parameters.toString());

		// The factory will be responsible for creating the threads to send, reflect, process results, etc.
		// Going to be lazy and use the same Map<String, Objects> parameters object to pass all the pertinent values around
		
		SolaceFactory solaceFactory = new SolaceFactory(parameters);
		Thread solaceFactoryThread = new Thread(solaceFactory);
		logger.debug("Solace Factory Thread created and being started.");
		solaceFactoryThread.start();
	}
	
	private static void setupLoggingProperties(Map<String,Object> parameters) {
		
		// Not going to refer to a log4j.properties file for this. Logging is either debug or not. That's it. A start-up arg can control it.
		// Also will be a rolling file, so set max size and the max count can be an argument with a default of 20? 30MB x 20 = Max disk space 600MB.
		final String maxLogSize = "30MB";

		
		// Define the pattern of the logging layout
		PatternLayout layout = new PatternLayout();
        String conversionPattern = "%-5p: %d [%t %c{1}] %m%n";
        layout.setConversionPattern(conversionPattern);
        
        // Get the Root Logger to configure it
        Logger rootLogger = Logger.getRootLogger();
        
        // Log to console or to a file?
        if (parameters.get("output_log") != null)
        {
        	// Create a file appender
        	RollingFileAppender fileAppender = new RollingFileAppender();
        	fileAppender.setFile(parameters.get("output_log").toString());
        	fileAppender.setMaxBackupIndex((int)parameters.get("output_log_rolled_count")); 
        	fileAppender.setMaxFileSize(maxLogSize);
        	fileAppender.setLayout(layout);
        	fileAppender.activateOptions();
        	 
        	rootLogger.addAppender(fileAppender);
        } 
        else
        {	
            // Create a console appender
            ConsoleAppender consoleAppender = new ConsoleAppender();
            consoleAppender.setLayout(layout);
            consoleAppender.activateOptions();
            
        	rootLogger.addAppender(consoleAppender);

        }
        

        // What level of logging requested?
        if ((boolean) parameters.get("debug"))
        {
    		rootLogger.setLevel(Level.DEBUG);
        }
        else
        {
    		rootLogger.setLevel(Level.INFO);
        }
	}
	
	private static void printStartBanner() {

		System.err.println("*************************************************");
		System.err.println("*************************************************");
		System.err.println("***  Latency Ping-Pong Program - "+ programVersion + "        ***");
		System.err.println("***                                           ***");
		System.err.println("***  Products/Protocols Supported:            ***");
		System.err.println("***   - Solace Messaging [JCSMP API: 10.6.3]  ***");
		System.err.println("***                                           ***");
		System.err.println("***  Contact: Jamil.Ahmed@Solace.com          ***");
		System.err.println("***                                           ***");
		System.err.println("***  DEMO-USE ONLY, NOT SUPPORTED BY SOLACE!  ***");
		System.err.println("***                                           ***");
		System.err.println("*************************************************");
		System.err.println("*************************************************");	
		
		// TODO: Add other API support such as AMQP JMS and MQTT alongside the Solace JCSMP API
	}
	
	/**
	 * This top level arguments parser will call other more specific ones to help it along.
	 * That way common args and product/transport specific args can be added as required in future.
	 * 
	 * @param args	String array of command-line arguments provided to the program
	 */
	private static Map<String,Object> parseArgs(String[] args) {
		
		
		// Where to save the parsed arguments as they go through the parsers?
		Map<String,Object> parsedArgs = new HashMap<String,Object>();
		
		// The Argument Parser that will be iteratively built up before using it
		ArgumentParser myArgParser = ArgumentParsers.newFor(programJarName).defaultFormatWidth(150).addHelp(false).build().defaultHelp(false);
		
		// Firstly, add the Solace specific arguments to the parser. 
		buildSolaceArgsParser(myArgParser);

		// Then build the parser for the common program arguments...
		buildCommonArgsParser(myArgParser);
				
		// Now ready to try and parse the arguments...
		try{				
			myArgParser.parseArgs(args, parsedArgs);
		}
		catch (ArgumentParserException e) {

			System.out.println("ERROR: Arguments Processing Exception. -> " + e.getMessage() + ".\n");
			myArgParser.printHelp();
			System.exit(0);
		}
		
		// Check if the interval value is too small. Enforce a floor value on it.
		int pingInterval = (int)parsedArgs.get("ping_interval");
		final int pingIntervalFloor = 50;
		if (pingInterval != 0 && pingInterval < 50)
		{
			parsedArgs.put("ping_interval", pingIntervalFloor);
		}
		
		
		// If the arguments did not provide explicit ping/pong topic names, setup default ones now with the node-name that is now sure to be present.
		if (parsedArgs.get("publish_topic") == null) {
			parsedArgs.put("publish_topic", pingTopicPrefix + "/" + parsedArgs.get("node_group") + "/" + parsedArgs.get("node_name"));
		}
		
		if (parsedArgs.get("subscribe_topic") == null) {
			parsedArgs.put("subscribe_topic", pingTopicPrefix + "/>," + pongTopicPrefix + "/>,");
		}
		
		if (parsedArgs.get("reflect_topic") == null) {
			parsedArgs.put("reflect_topic", pongTopicPrefix + "/" + parsedArgs.get("node_group") + "/" + parsedArgs.get("node_name"));
		}

		if (parsedArgs.get("results_topic") == null) {
			parsedArgs.put("results_topic", resultsTopicPrefix + "/" + parsedArgs.get("node_group") + "/" + parsedArgs.get("node_name"));
		}
		
		if (parsedArgs.get("summary_topic") == null) {
			parsedArgs.put("summary_topic", summaryTopicPrefix + "/" + parsedArgs.get("node_group") + "/" + parsedArgs.get("node_name"));
		}
		return parsedArgs;
	}
	
	/**
	 * This will build an arguments parser to deal with the common arguments that are independent on product/transport.
	 * These include args related to destinations to send/receive on, how many pings to send, how often, etc.
	 */
	private static void buildCommonArgsParser(ArgumentParser argCommonParser) {
				
		
		//Setup the arguments to expect, types and any default values
		
		// Create a group specific to these node identity arguments
		ArgumentGroup nodeArgGroup = argCommonParser.addArgumentGroup("Node Identity Arguments");
		
		// Need to know the hostname of where this is running to act as the default 'node name'.
		String detectedHostname;
		try {
			detectedHostname = InetAddress.getLocalHost().getHostName();
			nodeArgGroup.addArgument("-d", "--node-name")
					.help("The name to identify this ping-pong node with.	[Default: <Hostname> (" + detectedHostname + ") ]")
					.setDefault(detectedHostname);
		} catch (UnknownHostException e) {
			nodeArgGroup.addArgument("--name")
					.help("The name to identify this ping-pong node with.	(Required argument if hostname cannot be detected.)")
					.required(true);
		}
		
		nodeArgGroup.addArgument("-g", "--node-group")
			.help("Is this node part of a named group?		[Default: Common ]")
			.setDefault("Common");
		
		// Create a group just for the topic destinations
		ArgumentGroup topicsArgGroup = argCommonParser.addArgumentGroup("Topic Destination Arguments");
		topicsArgGroup.addArgument("-p", "--publish-topic")
				.help("Topic destination to send ping messages.	[Default: " + pingTopicPrefix + "/<node-group>/<node-name> ]");
		topicsArgGroup.addArgument("-s", "--subscribe-topic")
				.help("Topic destinations to receive messages.	[Default: " + pingTopicPrefix + "/>," + pongTopicPrefix + "/> ]");
		topicsArgGroup.addArgument("-r", "--reflect-topic")
				.help("Topic destination to reflect pong messages to.	[Default: " + pongTopicPrefix + "/<node-group>/<node-name> ]");
		topicsArgGroup.addArgument("-a", "--results-topic")
				.help("Topic destination to publish results to.	[Default: " + resultsTopicPrefix + "/<node-group>/<node-name> ]");
		topicsArgGroup.addArgument("-m", "--summary-topic")
				.help("Topic destination to publish summaries to.	[Default: " + summaryTopicPrefix + "/<node-group>/<node-name> ]");
		
		// Create a new group for the remainder
		ArgumentGroup commonArgGroup = argCommonParser.addArgumentGroup("Other Arguments");
		commonArgGroup.addArgument("-i", "--ping-interval")
				.help("Publish ping messages every N milliseconds.	[Default: 10000 (10 seconds), None: 0]")
				.type(Integer.class)
				.setDefault(10000);
		commonArgGroup.addArgument("-n", "--ping-count")
				.help("Publish a total of N ping messages.		[Default: 6, Unlimited: 0]")
				.type(Integer.class)
				.setDefault(6);
//		commonArgGroup.addArgument("-t", "--timeout")
//				.help("Program should auto-terminate after N seconds.	[Default: 120 (2 minutes), None: 0]")
//				.type(Integer.class)
//				.setDefault(120);
//		commonArgGroup.addArgument("-x", "--latency-summary-count")
//				.help("Summarise latency every N ping-pong results.	[Default: 6]")
//				.type(Integer.class)
//				.setDefault(6);
//		commonArgGroup.addArgument("-l", "--latency-summary-log")
//				.help("File location for summarised latency output.")
//				.type(Arguments.fileType()
//						.verifyNotExists().verifyCanCreate()
//						.or()
//						.verifyExists().verifyCanWrite());
		commonArgGroup.addArgument("-o", "--output-log")
				.help("Log file location for all program output. (Log will roll every 30MB.)")
				.type(Arguments.fileType()
						.verifyNotExists().verifyCanCreate()
						.or()
						.verifyExists().verifyCanWrite());
		commonArgGroup.addArgument("-k", "--output-log-rolled-count")
				.help("Maximum number of rolled logs to keep on disk.	[Default: 20]")
				.type(Integer.class)
				.setDefault(20);
		commonArgGroup.addArgument("--debug")
				.help("Enable debug level program output.")
				.type(boolean.class)
				.setDefault(false);
	}
	
	/**
	 * This will build an arguments parser to deal with the Solace specific arguments such as URLs, credentials, etc.
	 */
	private static void buildSolaceArgsParser(ArgumentParser argSolaceParser) {
		

		ArgumentGroup solaceArgGroup = argSolaceParser.addArgumentGroup("Solace Arguments");
		//Setup the arguments to expect, types and any default values
		solaceArgGroup.addArgument("-c", "--connection-url")
				.required(true)
        		.help("Hostname:Port of the Solace Message Router");
		solaceArgGroup.addArgument("-v", "--vpn")
				.help("VPN Name on the Solace Message Router")
				.setDefault("default");
		solaceArgGroup.addArgument("-u", "--username")
				.help("The username to connect to the Solace Message Router")
				.setDefault("default");
		solaceArgGroup.addArgument("-w", "--password")
				.help("The password to connect to the Solace Message Router")
				.setDefault("");

	}

}
