package com.itsjamilahmed.latencypingpong;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;

import com.solacesystems.jcsmp.InvalidPropertiesException;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPStreamingPublishEventHandler;
import com.solacesystems.jcsmp.JCSMPTransportException;
import com.solacesystems.jcsmp.XMLMessageProducer;

public class SolaceFactory implements Runnable {
	
	private Logger logger = Logger.getLogger(SolaceFactory.class);	// A log4j logger to handle all output
	private Map<String,Object> parameters;				// These control what the Factory needs to create
	private JCSMPSession session;						// There will be a shared Solace session per Factory. (1 connection to the message router used.)
	private XMLMessageProducer producer;							// Only one producer can exist per session, so need to share that too!
	private BlockingQueue<PingPongMessage> pingMessageProcessingQueue;	// A queue to hold messages that have been reflected back to the original source
	// TODO: Explore other better queue options for this?
	
	private boolean sessionCreated = false;
	private boolean sessionConnected = false;
	
	public SolaceFactory (Map<String,Object> parameters) {
		
		final int initialQueueSize = 100;				// Should be large enough to not fill up and block.
		this.parameters = parameters;
		pingMessageProcessingQueue = new ArrayBlockingQueue<PingPongMessage>(initialQueueSize);
	}
	
	private boolean createSolaceSession () {

		
		// This method can be called multiple times until the session is created and successfully connected.
		// Will return true when all successful.
		
		if (!sessionCreated)
		{
			// No session has been created yet...
			
			// Setup the session properties
			// Ref: https://docs.solace.com/API-Developer-Online-Ref-Documentation/java/com/solacesystems/jcsmp/JCSMPProperties.html
			final JCSMPProperties properties = new JCSMPProperties();
			properties.setProperty(JCSMPProperties.HOST, this.parameters.get("connection_url").toString());
			properties.setProperty(JCSMPProperties.USERNAME, this.parameters.get("username").toString());
			properties.setProperty(JCSMPProperties.PASSWORD, this.parameters.get("password").toString());
			properties.setProperty(JCSMPProperties.VPN_NAME,  this.parameters.get("vpn").toString());
			// If the session connection breaks and auto-connects, re-apply the topic subscriptions in case the router has timed them (and the connection) out.
			// Otherwise can get a situation where the connection is live but router has no subscriptions for it so no messages ever arrive.
			properties.setProperty(JCSMPProperties.REAPPLY_SUBSCRIPTIONS,  true);
			
			try {
				this.session = JCSMPFactory.onlyInstance().createSession(properties);
				sessionCreated = true;
				logger.debug("Session successfully created, will call connect()...");

			} catch (Exception e) {

				if(e instanceof InvalidPropertiesException){
					logger.error("Couldn't create Solace session due to invalid properties. " + e.getMessage());
//					logger.debug("Contents of properties object: " + properties.toString());
					// The above is quite a large output and seems more noise than usefulness...
				}
				else {
					logger.error("ERROR: An exception occured while creating the Solace session. Exception message -> " + e.getMessage());	
				}
			}
	
		}
		
				
		// If successful, try and connect it.
		if (sessionCreated) {
			try {
				
				this.session.connect();
				sessionConnected = true;
				// If successfully connected, get a XMLMessageProducer object too since that is to be shared by all threads
				// Will need to create an anonymous inner class of 'StreamingPublishEventHandler' for it
				
				this.producer = this.session.getMessageProducer(new JCSMPStreamingPublishEventHandler() {
					@Override
					public void responseReceived(String messageID) {
						// No responses expected for direct messages but have this ready as a stub for future development...
						logger.info("Producer received response for msg: " + messageID);
					}
					@Override
					public void handleError(String messageID, JCSMPException e, long timestamp) {
						
						if (e instanceof JCSMPTransportException){
							// This one is quite serious, means there was an issue on the underlying TCP connection.
							logger.error("A JCSMPTransportException occurred. Exception message -> " + e.getMessage());
							logger.debug("Stack Trace: ",e);

							// May as well terminate and start again
							logger.error("*** Program will terminate now. ***");
							System.exit(-1);
						}
						else
						{
							logger.error("Producer received error for msg: " + messageID + " @ " + timestamp + " - " + e);
							logger.debug("Stack Trace: ",e);

						}
					}
				});
			} 
			catch (Exception e) {
				if(e instanceof JCSMPTransportException){
					logger.error("Could not connect to the Solace Message Router at " + parameters.get("connection_url").toString());
					logger.debug("Stack Trace: ",e);

				}
				else if (e instanceof JCSMPException){
					logger.error("Could not create Message Producer from the Session. Exception message -> " + e.getMessage());
					logger.debug("Stack Trace: ",e);

				}
				else {
					logger.error("An exception occured in the Solace Factory. Exception message -> " + e.getMessage());	
					logger.debug("Stack Trace: ",e);

				}
			}
		}
		
		return sessionConnected;
	}
	
	@Override
	public void run() {
		
		// First create and connect the shared session for all the publisher and subscribe threads
		logger.debug("Creating Solace Session");
		
		// May need several attempts to create the session and need to make sure all is good before doing anything else.
		boolean sessionSuccess = false;
		int sessionConnectMaxAttempts = 10;		// How many times to try and connect
		int sessionConnectIntervalMs = 2000;	// How long to wait between attempts
		
		while (!sessionSuccess && sessionConnectMaxAttempts > 0)
		{
			sessionSuccess = this.createSolaceSession();
			sessionConnectMaxAttempts--;

			if (!sessionSuccess && sessionConnectMaxAttempts > 0)
			{
				logger.info("Failed to connect session. Will wait " + sessionConnectIntervalMs + "ms and try again. " + sessionConnectMaxAttempts + " further attempts remain.");
				try {
					Thread.sleep(sessionConnectIntervalMs);
				} catch (InterruptedException e) {
				}
			}
		}
		
		if (sessionSuccess)
		{
			// Start a Solace subscriber in its own thread
			logger.debug("Creating Solace Ping Subscriber and starting thread");
			SolacePingSubscriber solacePingSubscriber = new SolacePingSubscriber(parameters, session, producer, pingMessageProcessingQueue);
			Thread solaceSubcribeThread = new Thread(solacePingSubscriber);
			solaceSubcribeThread.start();
			
			// Is a ping publisher required?
			if ((int)parameters.get("ping_interval") != 0)
			{
				// Now start a Solace Ping publisher in its own thread
				logger.debug("Creating Solace Ping Publisher and starting thread");
				SolacePingPublisher solacePingPublisher = new SolacePingPublisher(parameters, producer);
				Thread solacePingThread = new Thread(solacePingPublisher);
				solacePingThread.start();
			}
			
			// Start a Solace results publisher in its own thread
			logger.debug("Creating Solace Results Publisher and starting thread");
			SolaceResultsPublisher solaceResultsPublisher = new SolaceResultsPublisher(parameters, producer, pingMessageProcessingQueue);
			Thread solaceResultsThread = new Thread(solaceResultsPublisher);
			solaceResultsThread.start();
		}
		else
		{
			logger.error("Failed to create a successful Solace session. Aborting Factory.");
		}

	}
		

}
