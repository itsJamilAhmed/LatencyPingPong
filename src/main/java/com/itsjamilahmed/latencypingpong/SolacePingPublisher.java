/**
 * 
 */
package com.itsjamilahmed.latencypingpong;

import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.log4j.Logger;

import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPTransportException;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.XMLMessageProducer;

/**
 * This class will create a Solace Ping Message Publisher
 * 
 * @author Jamil.Ahmed@Solace.com
 *
 */
public class SolacePingPublisher implements Runnable {

	private Logger logger = Logger.getLogger(SolacePingPublisher.class);	// A log4j logger to handle all output
	private XMLMessageProducer producer;
	private Map<String,Object> parameters;
	
	public SolacePingPublisher(Map<String,Object> parameters, XMLMessageProducer producer) {

		this.parameters = parameters;
		this.producer = producer;	// This class will be created with a reference to an existing valid and connected session and a producer from that session.
									// This is required in order to share a single session (connection to the message router) across threads 
									// and only one producer per session is allowed otherwise a new one closes an earlier one!
	}
	
	
	@Override
	public void run() {
				
		try {
			
			// Now get the topic created for publishing
			// Whether publish_topic has been specified or not will be checked earlier before getting here
			final Topic topic = JCSMPFactory.onlyInstance().createTopic(parameters.get("publish_topic").toString());

	        // Now we are ready to keep sending the ping messages
	        // Use a TimerTask to schedule the repetitive sends
	        
			// What is the maximum number of specified pings, unless it has been set to zero for unlimited.
 			String pingCount = (int) parameters.get("ping_count") == 0 ? "unlimited" : parameters.get("ping_count").toString();

 			// Print info message on what the publisher will be doing
	        logger.info("Publishing " + pingCount + " ping messages every " + parameters.get("ping_interval").toString() + " milliseconds on topic: " + topic.getName());
			
			TextMessage msg = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
			
			// Setup the Ping message that will be refreshed and sent with each Solace send call.
			// It's constructor wants to know the node name and group for identification. Optional Node Group name will be empty if nothing specified. No null-test needed.
			PingPongMessage pingMessage = new PingPongMessage(
					parameters.get("node_name").toString(),
					parameters.get("node_group").toString());
			
			Timer pingTimer = new Timer() ;
			
			Date firstTime = new Date(System.currentTimeMillis()+1000);	// By default, start after 1 second. May get overridden below...
			
			// Is this sending unlimited pings? If so, check how often pings are being sent and delay start time to a nice round time. (Create a "pretty" timestamp each time.)
			if ( (int) parameters.get("ping_count") == 0 )
			{
				int pingIntervalMs = (int) parameters.get("ping_interval");
				// Will delay till the nearest 5 seconds, 30 seconds, or 60. Only if the interval is a multiple of 5, or is the value 30 or 60.
				// Wont truncate the milliseconds to 0, no point moving all executions of this program to be at the same time. 
				if (pingIntervalMs >= 5000 && pingIntervalMs <= 60000)
				{
					if ((pingIntervalMs % 5000) == 0)
					{
						Calendar calendarStartTime = Calendar.getInstance();
						calendarStartTime.setTime(firstTime);
						switch (pingIntervalMs) {
							case 60000:
								// Zero the seconds and schedule for the next full minute
								calendarStartTime.set(Calendar.SECOND, 0);
								calendarStartTime.add(Calendar.MINUTE, 1);
								break;
							case 30000:
								// Just schedule to the next full 30 seconds
								if (calendarStartTime.get(Calendar.SECOND) <= 30) 
								{
									calendarStartTime.set(Calendar.SECOND, 30);
								}
								else
								{
									calendarStartTime.set(Calendar.SECOND, 0);
									calendarStartTime.add(Calendar.MINUTE, 1);
								}
								break;
							default:
								// Just schedule to the next nearest full 5 seconds (round up)
								int unroundedSeconds = calendarStartTime.get(Calendar.SECOND);
								int roundedSeconds = (int) Math.ceil(unroundedSeconds / 5) * 5;
								calendarStartTime.set(Calendar.SECOND, roundedSeconds);
						}
					
						firstTime = calendarStartTime.getTime();
						logger.info("Ping sending being delayed until: " + firstTime.toString());
					}
				}
				
			}

			
						
//			pingTimer.schedule(new TimerTask() {
			pingTimer.scheduleAtFixedRate(new TimerTask() {

				private long sendCounter = 0;
				private int maxPingCount = (int) parameters.get("ping_count");
				
				@Override
				public void run() {
					
					try {
						// Start at zero and increment to 1 on the very first iteration
						sendCounter++;
						
						// Keep refreshing and re-using the existing PingPongMessage object for each send....
						msg.setText(pingMessage.getPingMessage());	// The timestamps are refreshed on each call to this method.		
						producer.send(msg,topic);
						logger.debug("Successfully sent ping message: " + msg.getText());
					} catch (Exception e) {
						if (e instanceof JCSMPTransportException){
							// This one is quite serious, means there was an issue on the underlying TCP connection.
							logger.error("A JCSMPTransportException occurred. Exception message -> " + e.getMessage());
							logger.debug("Stack Trace: ",e);

							// May as well terminate and start again
							logger.error("*** Program will terminate now. ***");
							System.exit(-1);
						}
						else if (e instanceof JCSMPTransportException)
						{
							logger.error("A JCSMPException occurred on message send. Exception message ->" + e.getMessage());
							logger.debug("Stack Trace: ",e);

						}
						logger.error("An Exception occurred during message send. Exception message -> " + e.getMessage());
						logger.debug("Stack Trace: ",e);

					}					
					
					// How many times have I run?
					// If it was zero for unlimited, then this test will never see a zero for it to be true, so stays running forever
					if (sendCounter == maxPingCount) {
						// Have executed the maximum number of times, time to close the producer and cancel this thread.
						// Cancel means this particular run executes to the end, so needs to be cancelled after the message send.
						// Also cancel the original Timer, not the TimerTask if using this.cancel() which just cancels this run;
						logger.debug("Sent " + sendCounter + " ping messages, will cancel timer.");
						pingTimer.cancel();
					}
				}
			}, firstTime, Long.parseLong(parameters.get("ping_interval").toString())); // Start TimerTask at 'firstTime' and then repeat at given interval.


		} catch (Exception e) {
			if (e instanceof JCSMPException){
				logger.error("A JCSMPException occurred. Exception message -> " + e.getMessage());
				logger.debug("Stack Trace: ",e);
			}
			else if (e instanceof NullPointerException){
				// Shouldn't really trigger unless something has been coded wrong...
				logger.error("A NullPointerException occurred.");
				logger.debug("Stack Trace: ",e);
			}
			else {
				logger.error("An exception occurred. Exception message -> " + e.getMessage());
				logger.debug("Stack Trace: ",e);

			}
		}
	}
}
