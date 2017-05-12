package com.coverfox.bitserv;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ActionHandler {

  private static final Logger logger = LogManager.getLogger(BigQueryOps.class);
  BigQueryOps bqops = new BigQueryOps();

  public void handle(String message) {
    logger.info("Message received in handler");
    JSONObject msgObj = new JSONObject(message);
    String target = msgObj.getString("target");
    String action = msgObj.getString("action");
    JSONObject info = null;
    if (msgObj.has("info")) {
      info = msgObj.getJSONObject("info");
    }
    logger.info("Handing target: " + target + " action: " + action);
    switch (target) {

      case "dataset":
        switch (action) {
          case "create":
            bqops.createDataset(info.getJSONObject("data").getString("name"));
            break;
          case "rename":
            break;
          case "delete":
            break;
          default:
            break;
        }
        break;

      case "table":
        switch (action) {
          case "create":
            break;
          case "rename":
            break;
          case "drop":
            break;
          case "insert":
            break;
          case "update":
            break;
          case "delete":
            break;
          default:
            break;
        }
        break;
      default:
        break;
    }
  }
}
