package com.coverfox.bitserv;

import com.google.cloud.bigquery.InsertAllRequest;
import org.json.JSONObject;

import java.util.HashMap;


class Buffer{
  private HashMap<String, InsertAllRequest> buffer;
  
  public Buffer(){
    this.buffer = new HashMap<String, InsertAllRequest>();
  }
  public void flush(){
    this.buffer.clear();
  }
  public int getTotalMessages(){
    return this.buffer.size();
  }
  public void add( String table,InsertAllRequest request){
    this.buffer.put(table,request);
  }
}

// singleton
public class BatchInsertionControl{
  public static final int MAX_BUFFER_SIZE = 100;  // in messages
  public static final int MAX_BUFFER_TIME = 10;   // in seconds
  public static int lastInsertedTime;
  public static Buffer buffer;
  private static BatchInsertionControl instance = null;
  
  public static BatchInsertionControl getInstance(){
    if (instance == null) {
      instance = new BatchInsertionControl();
    }
    return instance;
  }

  private BatchInsertionControl(){
    this.buffer = new Buffer();
  }
  public String toString() {
    return getClass().getName() + "@" + Integer.toHexString(hashCode());
  }
  public boolean isBufferable(){
    if( this.buffer.getTotalMessages() < MAX_BUFFER_SIZE && !this.timerExpired() ) return true;
    return false;
  }
  public boolean timerExpired(){
    return false;
  }
  // check duplication ???
  public void buffer(JSONObject data,InsertAllRequest request){
    String dataset = data.getString("dataset");
    String table = data.getString("name");
    this.buffer.add(table,request); // add dataset also
  }
  public void cleanup(){
    this.buffer.flush();
  }
}


