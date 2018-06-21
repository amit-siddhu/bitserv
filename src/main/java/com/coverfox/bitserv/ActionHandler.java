package com.coverfox.bitserv;

import org.json.JSONObject;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.eventbus.AsyncEventBus;

public class ActionHandler {
  private static final Logger logger = LogManager.getLogger(BigQueryOps.class);
  private JSONObject message;
  private static BatchInsertionControl insertionControl; // remove this from action_handler
  private static AsyncEventBus eventbus;

  public ActionHandler(String message) {
    this.message = new JSONObject(message);
  }
  public static void initStaticDependecies(BatchInsertionControl controlInstance, AsyncEventBus e){
    insertionControl = controlInstance;
    eventbus = e;
  }

  public static void dispatchEvent(String event,String source){
    switch(event) {
      case "dispatch.buffer.all":
        if(insertionControl != null ){
          BigQueryOps.dispatchBatchInsertionsBasedOnSize(insertionControl);
        }
        break;
      case "dispatch.buffer.batch":
        if(insertionControl != null){
          BigQueryOps.dispatchSingleBatchInsertion(insertionControl);
        }
        break;
      default:
        logger.error("iEvent: [" + event + "] not found");
        break;
    }
  }

  public void handle() {
    String target = this.message.getString("target");
    String action = this.message.getString("action");

    switch (target) {
      case "dataset":
        switch (action) {
          case "create":
            new BigQueryOps(this.message.getJSONObject("data")).createDataset();
            break;
          case "update":
            new BigQueryOps(this.message.getJSONObject("data")).updateDataset();
            break;
          case "delete":
            new BigQueryOps(this.message.getJSONObject("data")).deleteDataset();
            break;
          default:
            logger.error("Action: [" + action + "] not found for target: [" + target + "]");
            break;
        }
        break;
      case "table":
        switch (action) {
          case "create":
            new BigQueryOps(this.message.getJSONObject("data")).createTable();
            break;
          case "update":
            new BigQueryOps(this.message.getJSONObject("data")).updateTable();
            break;
          case "insert":
            try{
              Integer bufferIndicator = insertionControl.buffer(this.message.getJSONObject("data"));
              MetricAnalyser.buffering();
              if(insertionControl.dispatchReady(bufferIndicator)) {
                ActionHandler.dispatchEvent("dispatch.buffer.batch","DISPATCHER"); // synchronous execution
                // eventbus.post(new BufferDispatchEvent()); // asynchronous execution
              }
            }catch(Exception e){
              logger.error("Dispatch Error : " + e.toString());
            }
            break;
          case "delete":
            new BigQueryOps(this.message.getJSONObject("data")).deleteTable();
            break;
          default:
            logger.error("Action: [" + action + "] not found for target: [" + target + "]");
            break;
        }
        break;
      default:
        logger.error("Target: [" + target + "] not found");
        break;
    }
  }
}
