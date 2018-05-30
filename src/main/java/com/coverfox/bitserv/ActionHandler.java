package com.coverfox.bitserv;

import org.json.JSONObject;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ActionHandler {
  private static final Logger logger = LogManager.getLogger(BigQueryOps.class);
  private JSONObject message;
  private static BatchInsertionControl insertionControl;
  private static final Object LOCK = new Object();

  public ActionHandler(String message) {
    this.message = new JSONObject(message);
  }
  public static void initStaticDependecies(BatchInsertionControl controlInstance){
    insertionControl = controlInstance;
  }
  public static void dispatchEvent(String event,String source){
    switch(event) {
      case "insert.buffer.dispatch":
        // System.out.println("***["+source+" Event]***");
        if(insertionControl != null && insertionControl.dispatchReady()){
          synchronized(LOCK){
            // System.out.println("***["+source+" LOCK]***");
            BigQueryOps.dispatchBatchInsertions(insertionControl);
            // System.out.println("***["+source+" UNLOCK]***");
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
            // System.out.println("***[BITSERVE EVENT]***");
            synchronized(LOCK){
              // System.out.println("***[BITSERVE LOCK]***");
              new BigQueryOps(this.message.getJSONObject("data")).processBatchInsertion(insertionControl);
              // System.out.println("***[BITSERVE UNLOCK]***");
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
