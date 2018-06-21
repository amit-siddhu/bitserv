package com.coverfox.bitserv;

import org.json.JSONObject;

public class BufferEvent extends Event{
	public BufferEvent(String eventname,JSONObject payload){
		super(eventname, payload);
	}
	public BufferEvent(JSONObject payload){
		super("buffer.add", payload);
	}
}