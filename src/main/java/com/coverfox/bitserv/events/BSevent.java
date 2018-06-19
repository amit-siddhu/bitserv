package com.coverfox.bitserv;

import org.json.JSONObject;

public class BSevent extends Event{
	public BSevent(String eventname, JSONObject payload){
		super(eventname, payload);
	}
}