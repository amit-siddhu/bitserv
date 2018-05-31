package com.coverfox.bitserv;

import org.json.JSONObject;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;



class Buffer{
  // {dataset : {table : [ requestString ] } }
  private HashMap<String, HashMap<String,ArrayList<JSONObject>>> buffer;
  private int totalEventsCached = 0; // current number of events in buffer
  public int totalEventsDispatched = 0;
  public Buffer(){
    this.buffer = new HashMap<>();
  }
  public void flush(){
    totalEventsCached = 0;
    this.buffer.clear();
  }
  public String toString(){
    HashMap<String,Integer> temp = new HashMap<String,Integer>();
    for(String dataset : this.buffer.keySet()){
      for(String table : this.buffer.get(dataset).keySet() ){
        temp.put(table,this.buffer.get(dataset).get(table).size());
      }
    }
    return temp.toString();
  }
  public int getTotalEventsCached(){
    return totalEventsCached;
  }
  public void add(String dataset, String table, JSONObject request){
    Map<String, ArrayList<JSONObject>> datasetBuffer;
    if(!this.buffer.containsKey(dataset)){
      this.buffer.put(dataset,new HashMap<String,ArrayList<JSONObject>>());
    }
    datasetBuffer = this.buffer.get(dataset);
    if(!datasetBuffer.containsKey(table)){
      datasetBuffer.put(table,new ArrayList<JSONObject>());
    }
    datasetBuffer.get(table).add(request);
    this.totalEventsCached += 1;
    this.totalEventsDispatched += 1;
  }
  public HashMap getCachedRequests(){
    return this.buffer;
  }
  public boolean isEmpty(){
    return this.totalEventsCached == 0;
  }
}

// singleton
public class BatchInsertionControl{
  private Integer bufferSize;// in messages
  private Buffer buffer;
  private static BatchInsertionControl instance = null;
  public static BatchInsertionControl getInstance(Integer bufferSize){
    if (instance == null) {
      instance = new BatchInsertionControl(bufferSize);
    }
    return instance;
  }
  private BatchInsertionControl(Integer bufferSize){
    this.buffer = new Buffer();
    this.bufferSize = bufferSize;
  }
  public int getEventsDispatchedCount(){
    return this.buffer.totalEventsDispatched;
  }
  public String toString(){
    return this.buffer.toString();
  }
  public boolean isBufferable(){
    if( this.buffer.getTotalEventsCached() < this.bufferSize ) return true;
    return false;
  }
  public boolean dispatchReady(){
    return !this.buffer.isEmpty();
  }
  public void buffer(JSONObject data){
    String dataset = data.getJSONObject("schema").getString("dataset");
    String table = data.getJSONObject("schema").getString("name");
    this.buffer.add(dataset, table, data);
  }
  public HashMap<String, HashMap<String,ArrayList<JSONObject>>>  getBufferedRequests(){
    return this.buffer.getCachedRequests();
  }
  public void cleanup(){
    this.buffer.flush();
  }
}


