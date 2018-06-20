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
  public Integer totalEventsDispatched = 0;
  private Integer capacity; // table level bound
  
  public Buffer(Integer capacity){
    this.buffer = new HashMap<>();
    this.capacity = capacity;
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
  public Integer add(String dataset, String table, JSONObject request){
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
    this.totalEventsDispatched += 1;
    if(this.totalEventsDispatched%10 == 0){
      System.out.println("buffer-10");
    }
    return datasetBuffer.get(table).size();
  }
  public HashMap getCachedRequests(){
    return this.buffer;
  }
  // public boolean dispatchReady(Integer bufferSize){
  //   for (String dataset : this.buffer.keySet()) {
  //     for (String table : this.buffer.get(dataset).keySet()){
  //       if(this.buffer.get(dataset).get(table).size() == this.capacity){
  //         return true;
  //       }
  //     }
  //   }
  //   return false;
  // }
}

// singleton
public class BatchInsertionControl{
  private Integer bufferSize;// in messages
  private Integer batchSize;// in messages
  private Buffer buffer;
  private static BatchInsertionControl instance = null;
  public static BatchInsertionControl getInstance(Integer batchSize, Integer capacityFactor){
    if (instance == null) {
      instance = new BatchInsertionControl(batchSize,capacityFactor);
    }
    return instance;
  }
  private BatchInsertionControl(Integer batchSize, Integer capacityFactor){
    this.bufferSize = capacityFactor * batchSize;
    this.buffer = new Buffer(this.bufferSize);
    this.batchSize = batchSize;
  }
  public Integer getBufferSize(){
    return this.bufferSize;
  }
  public Integer getBatchSize(){
    return this.batchSize;
  }
  public Buffer getBuffer(){
    return this.buffer;
  }
  public String toString(){
    return this.buffer.toString();
  }
  public boolean dispatchReady(Integer bufferIndicator){
    if(bufferIndicator > 0 && bufferIndicator % this.batchSize == 0){
      return true;
    }
    return false;
  }
  public Integer buffer(JSONObject data){
    String dataset = data.getJSONObject("schema").getString("dataset");
    String table = data.getJSONObject("schema").getString("name");
    return this.buffer.add(dataset, table, data);
  }
  public HashMap<String, HashMap<String,BlockingQueue<JSONObject>>>  getBufferedRequests(){
    return this.buffer.getCachedRequests();
  }
}


