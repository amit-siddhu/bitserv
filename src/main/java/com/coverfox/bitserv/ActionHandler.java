package com.coverfox.bitserv;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ActionHandler {

  private static final Logger logger = LogManager.getLogger(BigQueryOps.class);

  public static void handle(String message) {

    logger.info("Message received in handler: " + message);
    JSONObject msgObj = new JSONObject(message);
    String target = msgObj.getString("target");
    String action = msgObj.getString("action");
    JSONObject data = msgObj.getJSONObject("data");
    logger.info("Performing action: [" + action + "] on target: [" + target + "] with data: " + data);

    switch (target) {
      case "dataset":
        switch (action) {
          case "create":
            BigQueryOps.createDataset(data.getJSONObject("schema"));
            break;
          case "update":
            BigQueryOps.updateDataset(data.getJSONObject("schema"));
            break;
          case "delete":
            BigQueryOps.deleteDataset(data.getJSONObject("schema"));
            break;
          default:
            logger.error("Action: [" + action + "] not found for target: [" + target + "]");
            break;
        }
        break;
      case "table":
        switch (action) {
          case "create":
            BigQueryOps.createTable(data.getJSONObject("schema"));
            break;
          case "update":
            BigQueryOps.updateTable(data.getJSONObject("schema"));
            break;
          case "insert":
            BigQueryOps.insertAll(data.getJSONObject("schema"));
            break;
          case "delete":
            BigQueryOps.deleteTable(data.getJSONObject("schema"));
            break;
          default:
            logger.error("Action: [" + action + "] not found for target: [" + target + "]");
            break;
        }
        break;
      default:
        logger.error("Target: [" + target + "] not found");
    }
  }
}
