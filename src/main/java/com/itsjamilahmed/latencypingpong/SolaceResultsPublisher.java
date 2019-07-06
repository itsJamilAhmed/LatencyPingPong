/**
 * 
 */
package com.itsjamilahmed.latencypingpong;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.XMLMessageProducer;

/**
 * This class will create a Solace Results Publisher
 * 
 * @author Jamil.Ahmed@Solace.com
 *
 */
public class SolaceResultsPublisher implements Runnable {

	private Logger logger = Logger.getLogger(SolaceResultsPublisher.class);	// A log4j logger to handle all output

	private Map<String,Object> parameters;
	private XMLMessageProducer producer;
	private BlockingQueue<PingPongMessage> pingMessageProcessingQueue;	// The queue of final messages to calculate latency from
	
	
	public SolaceResultsPublisher(Map<String,Object> parameters, XMLMessageProducer producer, BlockingQueue<PingPongMessage> pingMessageProcessingQueue) {

		this.parameters = parameters;
		this.producer = producer;	// This class will be created with a reference to an existing Producer object and connected session
									// since that is shared by all publishing threads.

		this.pingMessageProcessingQueue = pingMessageProcessingQueue;	// The queue to get messages for latency calculations and summarising

	}
	
	private Map<String, Float> sortByValue(Map<String, Float> unsortedMap) {

        // 1. Convert Map to List of Map
        List<Map.Entry<String, Float>> list =
                new LinkedList<Map.Entry<String, Float>>(unsortedMap.entrySet());

        // 2. Sort list with Collections.sort(), provide a custom Comparator
        Collections.sort(list, new Comparator<Map.Entry<String, Float>>() {
            public int compare(Map.Entry<String, Float> o1,
                               Map.Entry<String, Float> o2) {
                return (o1.getValue()).compareTo(o2.getValue());
            }
        });

        // 3. Loop the sorted list and put it into a new insertion order Map LinkedHashMap
        Map<String, Float> sortedMap = new LinkedHashMap<String, Float>();
        for (Map.Entry<String, Float> entry : list) {
            sortedMap.put(entry.getKey(), entry.getValue());
        }

        /*
        //classic iterator example
        for (Iterator<Map.Entry<String, Integer>> it = list.iterator(); it.hasNext(); ) {
            Map.Entry<String, Integer> entry = it.next();
            sortedMap.put(entry.getKey(), entry.getValue());
        }*/
        return sortedMap;
    }
	
	@SuppressWarnings("unchecked")
	@Override
	public void run() {
				

			// (1) Get the topics created for publishing the results and summaries 
			final Topic resultsTopic = JCSMPFactory.onlyInstance().createTopic(parameters.get("results_topic").toString());
			final Topic summaryTopic = JCSMPFactory.onlyInstance().createTopic(parameters.get("summary_topic").toString());
			
			
 			// (2) Print info message on what the publisher will be doing
	        logger.info("Publishing results on topic: " + resultsTopic.getName() + " and summaries on topic: " + summaryTopic.getName());
	        
	        			
	        // (3) Create the TextMessage that will be used to send with
			TextMessage msg = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
			
			// (4) Get ready to start processing the queue and save the calculated latencies to a map of map
			PingPongMessage messageToProcess;
			
			Map<String,Float> individualNodeLatencies; 
			Map<String, Map<String, Float>> latenciesPerNodeGroup = new HashMap<String, Map<String, Float>>();
			Map<String,Float> tempNodeLatencies; 

			
			String pingID;
			String nodeGroup;
			String currentWorkingPingID = "";
			
			
			JSONObject groupSummary;		// Details of an individual group
			JSONObject allGroupsSummary = new JSONObject();	// List of all groups for a given ping timestamp
			
			// Keep looping to check the input queue for messages and process them
			while (true) 
			{
				try 
				{
					// (5) Get the message from the queue
					messageToProcess = 	pingMessageProcessingQueue.take();
					pingID = messageToProcess.getPingTimestamp();
					String shortTimestamp = messageToProcess.getPingShortTimestamp();
					
					// (6) Which ping are we working on?
					if (!pingID.equals(currentWorkingPingID))
					{
						// New ping has arrived. Determine the group winners of the previous ping and publish
						if (!currentWorkingPingID.equals(""))
						{
							logger.debug("New ping detected. Summarising rankings and latency of previous ping responses from " + latenciesPerNodeGroup.size() + " groups detected.");						
							
							// Iterate through each available ping-and-node-group available, then get the individual results inside and find the winner
							for (String group : latenciesPerNodeGroup.keySet())
							{
								groupSummary = new JSONObject();
								// Sort each group by latency
								tempNodeLatencies = this.sortByValue(latenciesPerNodeGroup.get(group));
								
								JSONArray rankedNodes = new JSONArray();

								for (String node : tempNodeLatencies.keySet())	// An ordered map of nodes with lowest latency first
								{
									rankedNodes.add(node);	// Create an array of just the node names inserted in order of latency
									groupSummary.put(node, tempNodeLatencies.get(node));	// Add the individual latency result too
								}
								groupSummary.put("ranking", rankedNodes);		// Insert the final ranked list
								allGroupsSummary.put(group, groupSummary);
							}
							
							// Publish the full summary for all groups now
							allGroupsSummary.put("timestamp", shortTimestamp);
							msg.setText(allGroupsSummary.toString());
							producer.send(msg,summaryTopic);
							
							logger.info("Published summary message: " + allGroupsSummary.toString());
							
							// Clear the map now for the new results to be entered
							latenciesPerNodeGroup.clear();
							allGroupsSummary.clear();
						}
						currentWorkingPingID = pingID;

					}

					// Which group is this response relating to?
					nodeGroup = messageToProcess.getReflectNodeGroup();

					// If existing results for that group exists, get that map
					if (latenciesPerNodeGroup.containsKey(nodeGroup))
					{
						individualNodeLatencies = latenciesPerNodeGroup.get(nodeGroup);
					}
					else
					{
						// First results being seen for this ping and group
						individualNodeLatencies = new HashMap<String, Float>();
					}

					// Check if maybe there are multiple reflectors configured with the same node name and group.
					// No use in knocking out the latency results of an earlier one with a later arrival if that's the case
					
					if (!individualNodeLatencies.containsKey(messageToProcess.getReflectNodeName()) )
					{
						// Put this particular node's results into a map that contains results for this *group* only
						individualNodeLatencies.put(messageToProcess.getReflectNodeName(), Float.valueOf(messageToProcess.getPingLatencyMsString()));

						// Then put this updated map of results for this group into the parent map of all results
						latenciesPerNodeGroup.put(nodeGroup, individualNodeLatencies);

						// Publish the results for this node too
						msg.setText(messageToProcess.getLatencyResultsMessage());
						producer.send(msg,resultsTopic);
						logger.info(messageToProcess.getLatencyResultsMessage());
	
					}
					else
					{
						// Will ignore this later message even though it arrived.
						// Use case could be multiple nodes running with the same name in competition or for resiliency?
					}
					

				}
				catch (Exception e) 
				{
					if (e instanceof JCSMPException)
					{
						logger.error("A JCSMPException occurred. Exception message -> " + e.getMessage());
					}
					else if (e instanceof NullPointerException){
						// Shouldn't really be triggering this in normal operation...
						logger.error("A NullPointerException occurred.");
						logger.error(e.getStackTrace().toString());
					}
					else 
					{
						logger.error("An exception occurred. Exception message -> " + e.getMessage());			
					}	
				}
				
			}			
	}

}
