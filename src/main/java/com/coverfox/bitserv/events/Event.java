package com.coverfox.bitserv;

import org.json.JSONObject;

public class Event{
	public String name;
	public JSONObject message;
	
	public Event(String eventname, JSONObject payload){
		this.name = eventname;
		this.message = payload;
	}
	public Event(String eventname){
		this.name = eventname;
		this.message = new JSONObject("{}");
	}

	public String getName(){
		return this.name;
	}
	public JSONObject getMessage(){
		return this.message;
	}
	public String toString(){
		return this.name;
	}
}