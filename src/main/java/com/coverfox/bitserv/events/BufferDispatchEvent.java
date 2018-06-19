package com.coverfox.bitserv;

import org.json.JSONObject;

public class BufferDispatchEvent extends Event{
	public boolean dispatchAll = false;
	public BufferDispatchEvent(){
		super("buffer.dispatch");
	}
	public BufferDispatchEvent(boolean dispatchAll){
		super("buffer.dispatch");
		this.dispatchAll = dispatchAll;
	}
}