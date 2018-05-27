package com.coverfox.bitserv;

import org.json.JSONObject;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Timer;
import java.util.TimerTask;

public class ActionHandler {

  private static final Logger logger = LogManager.getLogger(BigQueryOps.class);
  private JSONObject message;
  private static BatchInsertionControl insertionControl;
  private static BatchInsertionTimer timerInstance;

  public ActionHandler(String message) {
    this.message = new JSONObject(message);
    insertionControl = BatchInsertionControl.getInstance(); // do bufferring or know when to perform insertion
    BatchInsertionTimer.initTimer(insertionControl.MAX_BUFFER_TIME); // singleton
  }
  public static void dispatchEvent(String event){
    switch(event) {
      case "insert.buffer.dispatch": // thread safe operation
        System.out.println("Found reminent requests : " + insertionControl.toString());
        BigQueryOps.dispatchBatchInsertions(insertionControl);
        System.out.println("After dispatch, reminent requests : " + insertionControl.toString());
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
            new BigQueryOps(this.message.getJSONObject("data")).processBatchInsertion(insertionControl);
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


class BatchInsertionTimer {
  private Timer timer;
  private BatchInsertionTimer(int seconds) {
    this.timer = new Timer();
    this.tick(seconds);
  }
  private void tick(int seconds){
    this.timer.schedule(new TimerTask() {
      @Override
      public void run() {
        ActionHandler.dispatchEvent("insert.buffer.dispatch");
        // System.out.println("tick");
      }
    }, seconds*1000,seconds*1000);
  }
  private static BatchInsertionTimer instance = null;

  public static BatchInsertionTimer initTimer(int seconds){
    if (instance == null){
      instance = new BatchInsertionTimer(seconds);
    }
    return instance;
  }
}
