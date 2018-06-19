package com.coverfox.bitserv;
import org.json.JSONObject;

public class SimpleEvent extends Event{
	public SimpleEvent(String eventname, JSONObject payload){
		super(eventname, payload);
	}
}