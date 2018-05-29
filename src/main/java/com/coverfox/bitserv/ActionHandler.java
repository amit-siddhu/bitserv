package com.coverfox.bitserv;

import org.json.JSONObject;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ActionHandler {
  // debugging purposr
  private static void sleep(int time){
    try{
      Thread.sleep(time * 1000);
    }catch(InterruptedException ex) {
      Thread.currentThread().interrupt();
    }
  }
  private static final Logger logger = LogManager.getLogger(BigQueryOps.class);
  private JSONObject message;
  private static BatchInsertionControl insertionControl;
  private static BatchInsertionTimer timerInstance;
  private static final Object LOCK = new Object();

  public ActionHandler(String message) {
    this.message = new JSONObject(message);
  }
  public static void initStaticDependecies(BatchInsertionControl controlInstance){
    insertionControl = controlInstance;
  }
  public static void dispatchEvent(String event,String source){
    switch(event) {
      case "insert.buffer.dispatch": // thread safe operation
        // System.out.println("***["+source+"]*** EVENT");
        if(insertionControl != null && insertionControl.dispatchReady()){
          synchronized(LOCK){
            // System.out.println("***["+source+"]*** LOCK");
            // System.out.println("Found reminent requests : " + insertionControl.toString());
            // ActionHandler.sleep(3);
            BigQueryOps.dispatchBatchInsertions(insertionControl);
            // System.out.println("After dispatch, reminent requests : " + insertionControl.toString() + " ; dispatchCount : "+ Integer.toString(insertionControl.getEventsDispatchedCount()) );
            // System.out.println("***["+source+"]*** UNLOCK");

          }
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
            synchronized(LOCK){
              // System.out.println("***[RABBITMQ]*** LOCK");
              new BigQueryOps(this.message.getJSONObject("data")).processBatchInsertion(insertionControl);
              // System.out.println("***[RABBITMQ]*** UNLOCK");

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
