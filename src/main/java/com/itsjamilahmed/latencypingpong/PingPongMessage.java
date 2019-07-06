package com.itsjamilahmed.latencypingpong;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

// This was done to avoid warnings related to Type safety. JSONObject extends HashMap but doesn't support Generics, so Eclipse IDE gives warning
// See https://stackoverflow.com/questions/16415436/json-and-generics-in-java-type-safety-warning
@SuppressWarnings("unchecked") 
public class PingPongMessage {
	
	private JSONObject jsonMessage; 			// This will contain the fields that will get sent/received between nodes.
	private String nodeName = "";				// This is to identify who is creating this object, regardless of being a sender or receiver.
	private String nodeGroupName = "";			// and optionally the group name of the node too.
	private boolean discard = false;			// A flag to mark if the message is corrupt or needing to be ignored for whatever reason.
	private boolean reflectRequired = true;		// A boolean to quickly check if a reflect is required without having to interrogate the JSONObject.
	private long calculatedLatencyNs = -1;		// Save the calculated latency after the first time it is asked for
	private long calculatedLatencyMs = -1;		// Save the calculated latency after the first time it is asked for using the alternative method

	private JSONObject resultsJsonMessage;		// A simplified json message of the final latency results
	
	private final int nanosecondsToMillisecondsDiv = 1000000;

	private DateFormat dateFormatMillis = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS z");
	private DateFormat dateFormatShort = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");



	private PingPongMessage() {
		jsonMessage = new JSONObject();
		jsonMessage.put("r", false);				// Reflected Message is false if created through a constructor
		jsonMessage.put("v", true);					// Valid Message is true when created through the constructor
		this.setDiscard(false);
	}
	
	public PingPongMessage(String nodeName) {
		this();
		this.nodeName = nodeName;					// At this point we don't know yet if this is a ping sender or a pong reflector
	}

	public PingPongMessage(String nodeName, String nodeGroupName) {
		
		this(nodeName);
		this.nodeGroupName = nodeGroupName;			// At this point we don't know yet if this is a ping sender or a pong reflector
	}
	
	public PingPongMessage(String nodeName, String nodeGroupName, String receivedMessage) {
		
		this(nodeName, nodeGroupName);
		this.receiveMessage(receivedMessage);
	}
	
	public boolean receiveMessage (String receivedMessage) {
		
		try {
			jsonMessage = (JSONObject) new JSONParser().parse(receivedMessage);
			
			// (1) Is the parsed message a valid one? If not then do nothing, let the calling app decide what to do with it.
			Boolean validMessage = (Boolean) jsonMessage.get("v");
			if (validMessage != null && !validMessage.booleanValue() )
			{
				// Message was valid JSON but not one that this class created. Do nothing and just return.
				this.setDiscard(true);
				this.setReflectRequired(false);
				return this.isReflectRequired();	// Not to be reflected back either
			} 
			else 
			{
				this.setDiscard(false);
			}
			
			
			// (2) Now to check if the message that got received needs to be reflected back or already a reflect that is returning to original source
			if (! (boolean) jsonMessage.get("r")) 
			{
				// Not a previous reflect, so flip the boolean and set details of who is reflecting the message.
				this.setReflectRequired(true);
				jsonMessage.put("r", true);
				jsonMessage.put("rn", this.nodeName);		// Node Name of the Pong Message Reflector
				jsonMessage.put("rg", this.nodeGroupName);	// Node group name of the Pong Message Reflector
				return this.isReflectRequired();
			}
			else
			{
				// (3) Already a reflect message that needs no further reflect
				this.setReflectRequired(false);
				
				// Now if it is back at the original source record the time it arrived so it can be processed later to measure latency
				if (jsonMessage.get("n").toString().equals(this.nodeName) && jsonMessage.get("g").toString().equals(this.nodeGroupName))
				{
					// System.out.println("INFO: Found a message that came back to the original source. " + jsonMessage.toString());
					// Collect the receive timestamp and then return. No need to reflect this or do anything here at this point.
					jsonMessage.put("rns", System.nanoTime());				// Get the nanos first
					jsonMessage.put("rms", System.currentTimeMillis());
					return this.isReflectRequired();
				} 
				else
				{
					// Not at the original source so ignore the message, nothing to do.
					this.setDiscard(true);
					return this.isReflectRequired();
				}

			}

		} catch (ParseException e) {
			// Should the fact that the reflect message was corrupt be signalled in a field within the newly created message?
			jsonMessage = new JSONObject();			// Need to create an empty one to reset if previously populated
			jsonMessage.put("v", false);			// Valid Message = False
			this.setDiscard(true);
			return this.isReflectRequired();
		}
	}
	
	public void setSenderNodeName(String nodeName)
	{
		jsonMessage.put("n", nodeName);
	}
	
	public void setSenderNodeGroupName(String nodeGroupName)
	{
		jsonMessage.put("g", nodeGroupName);
	}
	

	
	public String getPongMessage()
	{
		return this.jsonMessage.toString();			// Provide message contents as-is.
	}
	
	public String toString()
	{
		return this.getPongMessage();				// Provide message contents as-is.
	}
	
	public String getPingMessage()
	{
		// This call tells us that this is a Ping Sender. So set the node fields if this is first call and so not set yet...
		if (jsonMessage.get("n") == null) {
			jsonMessage.put("n", this.nodeName);			// Node Name of the Ping Sender
			jsonMessage.put("g", this.nodeGroupName);		// Was the optional group name set? Stays an empty string if not set to anything. (Null not possible.)
			
//			if (this.nodeGroupName != null) {
//				jsonMessage.put("g", this.nodeGroupName);
//			}
		}
				
		this.refreshMessage();						// Update the timestamps before providing the contents
		return this.jsonMessage.toString();
	}
	
	private void refreshMessage()
	{
		jsonMessage.put("ns", System.nanoTime());			// Get the nanos first
		jsonMessage.put("ms", System.currentTimeMillis());
	}

	public boolean isDiscard() {
		return discard;
	}

	private void setDiscard(boolean discard) {
		this.discard = discard;
	}

	public boolean isReflectRequired() {
		return reflectRequired;
	}

	private void setReflectRequired(boolean reflect) {
		this.reflectRequired = reflect;
	}
	
	public String getReflectNodeGroup() {
		
		if (this.jsonMessage.containsKey("rg"))
		{
			return this.jsonMessage.get("rg").toString();
		}
		else
		{
			return "";
		}
	}
	
	public String getReflectNodeName() {
		
		if (this.jsonMessage.containsKey("rn"))
		{
			return this.jsonMessage.get("rn").toString();
		}
		else
		{
			return "";
		}
	}
	
	public String getPingTimestamp() {
		
		if (this.jsonMessage.containsKey("ms"))
		{
			return dateFormatMillis.format(
					new Date( (long) this.jsonMessage.get("ms") ));
		}
		else
		{
			return "";
		}
	}

	public String getPingShortTimestamp() {
		
		if (this.jsonMessage.containsKey("ms"))
		{
			return dateFormatShort.format(
					new Date( (long) this.jsonMessage.get("ms") ));
		}
		else
		{
			return "";
		}
	}
	public float getPingLatencyNs() {
		
		if (calculatedLatencyNs == -1)
		{
			if (this.jsonMessage.containsKey("rns"))
			{
				calculatedLatencyNs =  (long)(this.jsonMessage.get("rns")) - (long)(this.jsonMessage.get("ns"));
			}	
		}
		return calculatedLatencyNs;		// Will stay unchanged at -1 if there was no calculation performed
		
	}
	
	public float getPingLatencyMs() {
		
		return getPingLatencyNs() / nanosecondsToMillisecondsDiv;
		
	}
	
	public String getPingLatencyMsString() {
		
		
		return String.format(java.util.Locale.US,"%.3f", getPingLatencyMs());
		
	}

	public String getPingLatencyMsStringAlternative() {
		
		// System.nanoTime() comparisons can be unreliable depending on the OS and architecture being run on.
		// Running on Linux should be fine, keep this alternative that just uses the System.currentTimeMillis() as a backup
		
		if (calculatedLatencyMs == -1)
		{
			if (this.jsonMessage.containsKey("rms"))
			{
				calculatedLatencyMs = (long)this.jsonMessage.get("rms") - (long)this.jsonMessage.get("ms");
			}	
		}
		return Long.toString(calculatedLatencyMs);		// Will stay unchanged at -1 if there was no calculation performed
		
	}
	
	
	public String getLatencyResultsMessage() {
		
		// This will return empty if there are no results applicable.
		if (! this.reflectRequired)
		{	
			if (this.resultsJsonMessage == null)
			{
				this.resultsJsonMessage = new JSONObject();

				resultsJsonMessage.put("timestamp", this.getPingTimestamp());

				resultsJsonMessage.put("from", this.nodeGroupName + ":" + this.nodeName);
				resultsJsonMessage.put("to", this.getReflectNodeGroup() + ":" + this.getReflectNodeName());
				resultsJsonMessage.put("rtt", this.getPingLatencyMsString());
			}
			
			return resultsJsonMessage.toString();
		}
		else
		{
			return "";
		}
	}
}
