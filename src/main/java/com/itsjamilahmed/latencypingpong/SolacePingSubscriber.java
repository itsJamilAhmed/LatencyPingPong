/**
 * 
 */
package com.itsjamilahmed.latencypingpong;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;

import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPTransportException;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.XMLMessageConsumer;
import com.solacesystems.jcsmp.XMLMessageListener;
import com.solacesystems.jcsmp.XMLMessageProducer;

/**
 * This class will create a Solace Ping Message Subscriber
 * 
 * @author Jamil.Ahmed@Solace.com
 *
 */
public class SolacePingSubscriber implements Runnable {

	private Logger logger = Logger.getLogger(SolacePingSubscriber.class);	// A log4j logger to handle all output
	private JCSMPSession session;
	private XMLMessageProducer producer;
	private Map<String,Object> parameters;
	private BlockingQueue<PingPongMessage> pingMessageProcessingQueue;
	
	public SolacePingSubscriber(Map<String,Object> parameters, JCSMPSession session, XMLMessageProducer producer,
			BlockingQueue<PingPongMessage> pingMessageProcessingQueue) {

		this.parameters = parameters;
		this.session = session;		// This class will be created with a reference to an existing valid and connected session.
		this.pingMessageProcessingQueue = pingMessageProcessingQueue;	// The queue of final messages to calculate latency from
		this.producer = producer;	// This class will be created with a reference to an existing Producer object and connected session
									// since that is shared by all publishing threads.
	}
	
	
	@Override
	public void run() {
				
		try {
			
			// The inner class will need to know this information to create the PingPongMessage objects
	        final String myNodeName = parameters.get("node_name").toString();
	        final String myNodeGroupName = parameters.get("node_group").toString();		// This will be empty string if never set. Don't need to test for null.
	        
			final CountDownLatch latch = new CountDownLatch(1); // Use a latch to keep this subscriber thread running until countDown() is called somewhere else.
            
			// (1) First setup the producer that will be used to reflect messages back
			final Topic reflectTopic = JCSMPFactory.onlyInstance().createTopic(parameters.get("reflect_topic").toString());
			logger.info("Publishing reflected pong messages on topic: " + reflectTopic.getName());
			TextMessage reflectMsg = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
			
			
			// Get a Message Consumer Object from the session
			// An events listener is required when getting a Message consumer for asynchronous callback. (With an onReceive and onException defined.)
			// Create an anonymous inner class for that upon requesting it.
			
			final XMLMessageConsumer cons = session.getMessageConsumer(new XMLMessageListener() {
	            @Override
	            public void onReceive(BytesXMLMessage msg) {
	            	
	            	// Just expecting TextMessage format for this program, everything else just ignore it.
	                if (msg instanceof TextMessage) {
	                    logger.debug("TextMessage received: " + ((TextMessage)msg).getText());

	                	PingPongMessage receivedMessage = new PingPongMessage(myNodeName, myNodeGroupName, ((TextMessage)msg).getText());
	                	// Message will return true for isReflectRequired if the message is needing to be reflected.
	                	if (receivedMessage.isReflectRequired()) {
	                		
	                		// Reflect it back using the Producer object
	                		reflectMsg.setText(receivedMessage.getPongMessage());
							try {
								producer.send(reflectMsg,reflectTopic);
							} catch (Exception e) {
								if (e instanceof JCSMPTransportException){
									// This one is quite serious, means there was an issue on the underlying TCP connection.
									logger.error("A JCSMPTransportException occurred. Exception message -> " + e.getMessage());
									// May as well terminate and start again
									logger.error("*** Program will terminate now. ***");
									System.exit(-1);
								}
								else if (e instanceof JCSMPException)
								{
									logger.error("A JCSMPException occurred on reflect message send. Exception message ->" + e.getMessage());
								}
								logger.error("An Exception occurred during reflect message send. Exception message -> " + e.getMessage());
							}
							
							logger.debug("Successfully sent reflect message: " + reflectMsg.getText());	
							
	                	} 
	                	else 
	                	{
	                		if (!receivedMessage.isDiscard()) {
	                			// The message is not needing to be reflected back, nor has it been marked for discard due to corruption
	                			
		                		// Offer to insert into the queue if it is not full. Return immediately without blocking.
		                		// Will return false if failed to insert, but ignore that and move on if any problems.
	                			pingMessageProcessingQueue.offer(receivedMessage);
		                		logger.debug("A received message has been added to the latency processing queue. Current queue depth: " + pingMessageProcessingQueue.size());
	                		}
	                	}
	                    
	                }
	                else
	                {
	                	// Received a message not in the expected format.
	                	logger.debug("Received and ignored a message not in the expected TextMessage format: " + msg.dump());
	                }
	                //latch.countDown();  // Keep this thread running forever
	            }

	            @Override
	            public void onException(JCSMPException e) {
	            	if (e instanceof JCSMPTransportException){
						// This one is quite serious, means there was an issue on the underlying TCP connection.
						logger.error("A JCSMPTransportException occurred. Exception message -> " + e.getMessage());
						// May as well terminate and start again
						logger.error("*** Program will terminate now. ***");
						System.exit(-1);
					}
	            	else
	            	{
	            		logger.error("Consumer received an exception: " + e);
	            	}
	                
	                //latch.countDown();  // unblock main thread so it can exit. Or what if the error is recoverable?
	            }
	        });
			
			// What topic is this subscriber interested in? Get a topic object from the JCSMP Factory with that name/pattern
			
			for (String topicString: parameters.get("subscribe_topic").toString().split(","))
			{
				Topic topic = JCSMPFactory.onlyInstance().createTopic(topicString);
				session.addSubscription(topic);
			}
			
	        // Now we are ready to receive messages...
 			// Print info message on what the subscriber will be doing
	        logger.info("Subscribing for ping messages on topics: " + parameters.get("subscribe_topic").toString());
			cons.start();
			logger.debug("Consumer object has been started successfully");
			
	        try {
	            latch.await(); // block here until message received, and latch will flip
	        } catch (InterruptedException e) {
	        	// When will this get triggered?
	            logger.info("Subscriber thread was awoken.");
	        }

		} catch (Exception e) {
			if (e instanceof JCSMPException){
				logger.error("A JCSMPException occurred. Exception message -> " + e.getMessage());
			}
			else {
				logger.error("An exception occurred. Exception message -> " + e.getMessage());			
			}
		}
	}
}
