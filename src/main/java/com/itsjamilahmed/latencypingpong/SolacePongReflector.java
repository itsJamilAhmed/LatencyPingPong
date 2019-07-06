/**
 * 
 */
package com.itsjamilahmed.latencypingpong;

import java.util.Map;
import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;

import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.XMLMessageProducer;

/**
 * This class will create a Solace Pong Message Reflect Publisher
 * 
 * @author Jamil.Ahmed@Solace.com
 *
 */
public class SolacePongReflector implements Runnable {

	private Logger logger = Logger.getLogger(SolacePongReflector.class);	// A log4j logger to handle all output

	private Map<String,Object> parameters;
	private XMLMessageProducer producer;
	private BlockingQueue<PingPongMessage> pongMessageOutputQueue;	// The queue to get messages that are to be reflected back
	private boolean queueSpin = false;								// Controls the behaviour of how to process the queue. Spin on it or poll.

	
	public SolacePongReflector(Map<String,Object> parameters, XMLMessageProducer producer, BlockingQueue<PingPongMessage> pongMessageOutputQueue) {

		this.parameters = parameters;
		this.producer = producer;	// This class will be created with a reference to an existing Producer object and connected session
									// since that is shared by all publishing threads.

		this.pongMessageOutputQueue = pongMessageOutputQueue;	// The queue to get messages that are to be reflected back

	}
	
	
	@Override
	public void run() {
				

			// (1) Get the topic created for publishing
			final Topic topic = JCSMPFactory.onlyInstance().createTopic(parameters.get("reflect_topic").toString());

	        // Now we are ready to keep processing the queue and reflect back 'pong' messages

 			// (2) Print info message on what the publisher will be doing
	        logger.info("Publishing reflected pong messages on topic: " + topic.getName());
	        if (this.isQueueSpin())
	        {
				logger.debug("Processing queue using non-blocking poll()");
	        }
	        else
	        {
				logger.debug("Processing queue using blocking take()");

	        }
			
	        // (3) Create the TextMessage that will be used to send with
			TextMessage msg = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
			
			// (4) Now start processing the queue
			
			// Keep looping to check the input queue for messages
			while (true) 
			{
				try 
				{
					// Get messages off the queue in a low-latency (but CPU using) spinning method?							
					if (this.isQueueSpin()) 
					{
						PingPongMessage dequeuedMessage =  this.pongMessageOutputQueue.poll();
						if (dequeuedMessage != null)
						{
							msg.setText(dequeuedMessage.getPongMessage());
							producer.send(msg,topic);
							logger.debug("Successfully sent reflect message: " + dequeuedMessage.getPongMessage());					
						}		
					}
					// Block on the queue, to wake up thread when a message arrives. (Not lowest latency reflection.)
					else 
					{
						msg.setText(this.pongMessageOutputQueue.take().getPongMessage());
						producer.send(msg,topic);
						logger.debug("Successfully sent reflect message: " + msg.getText());	
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


	public boolean isQueueSpin() {
		return queueSpin;
	}


	public void setQueueSpin(boolean queueSpin) {
		this.queueSpin = queueSpin;
	}
}
