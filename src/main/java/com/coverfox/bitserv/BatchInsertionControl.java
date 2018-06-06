package com.coverfox.bitserv;

import org.json.JSONObject;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.LinkedList;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;


class Buffer{
  // {dataset : {table : [ requestString ] } }
  private HashMap<String, HashMap<String,BlockingQueue<JSONObject>>> buffer;
  private Integer totalEventsCached = 0; // current number of events in buffer
  public Integer totalEventsDispatched = 0;
  private Integer capacity; // table level bound
  
  public Buffer(Integer capacity){
    this.buffer = new HashMap<>();
    this.capacity = capacity;
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
  public void add(String dataset, String table, JSONObject request){
    Map<String, BlockingQueue<JSONObject>> datasetBuffer;
    if(!this.buffer.containsKey(dataset)){
      this.buffer.put(dataset,new HashMap<String,BlockingQueue<JSONObject>>());
    }
    datasetBuffer = this.buffer.get(dataset);
    if(!datasetBuffer.containsKey(table)){
      datasetBuffer.put(table,new ArrayBlockingQueue<JSONObject>(this.capacity));
    }
    try {
      datasetBuffer.get(table).put(request);
    }catch(InterruptedException e){
      e.printStackTrace();
    }
    this.totalEventsCached += 1;
    this.totalEventsDispatched += 1;
  }
  public HashMap getCachedRequests(){
    return this.buffer;
  }
  // change method name [MISLEADING]
  public boolean dispatchReady(){
    for (String dataset : this.buffer.keySet()) {
      for (String table : this.buffer.get(dataset).keySet()){
        if(this.buffer.get(dataset).get(table).size() > this.capacity){
          return false;
        }
      }
    }
    return true;
  }
  public int getTotalEventsCached(){
    return totalEventsCached;
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
    this.buffer = new Buffer(bufferSize*2); 
    this.bufferSize = bufferSize;
  }
  public Integer getBufferSize(){
    return this.bufferSize;
  }
  public Buffer getBufferDataStructure(){
    return this.buffer;
  }
  // for debugging
  public int getEventsDispatchedCount(){
    return this.buffer.totalEventsDispatched;
  }
  public String toString(){
    return this.buffer.toString();
  }
  public boolean dispatchReady(){
    return this.buffer.dispatchReady();
  }
  public void buffer(JSONObject data){
    String dataset = data.getJSONObject("schema").getString("dataset");
    String table = data.getJSONObject("schema").getString("name");
    this.buffer.add(dataset, table, data);
  }
  public HashMap<String, HashMap<String,BlockingQueue<JSONObject>>>  getBufferedRequests(){
    return this.buffer.getCachedRequests();
  }
  public void cleanup(){
    this.buffer.flush();
  }
}


