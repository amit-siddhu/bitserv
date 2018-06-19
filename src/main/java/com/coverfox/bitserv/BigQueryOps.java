package com.coverfox.bitserv;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQuery.DatasetDeleteOption;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;

public class BigQueryOps {

  private static final Logger logger = LogManager.getLogger(BigQueryOps.class);
  private static final BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
  private JSONObject data;

  public BigQueryOps(JSONObject data) {
    this.data = data;
  }

  public Dataset createDataset() {
    Dataset dataset = null;
    DatasetInfo datasetInfo = DatasetInfo.newBuilder(
      this.data.getJSONObject("schema").getString("name")
    ).build();
    try {
      dataset = bigquery.create(datasetInfo);
      logger.info("[CREATE_DATASET_SUCCESS]: " + dataset);
    } catch (BigQueryException e) {
      logger.error("[CREATE_DATASET_ERROR]: " + e);
    }
    return dataset;
  }

  public Dataset updateDataset() {
    JSONObject jDatasetSchema = this.data.getJSONObject("schema");
    String datasetName = jDatasetSchema.getString("name");
    String newFriendlyName = jDatasetSchema.getString("newFriendlyName");
    Dataset newDataset = null;
    Dataset oldDataset = bigquery.getDataset(datasetName);
    DatasetInfo datasetInfo = oldDataset.toBuilder().setFriendlyName(newFriendlyName).build();
    try {
      newDataset = bigquery.update(datasetInfo);
      logger.info("[UPDATE_DATASET_SUCCESS]: " + newDataset);
    } catch (BigQueryException e) {
      logger.error("[UPDATE_DATASET_ERROR]: " + e);
    }
    return newDataset;
  }

  public Boolean deleteDataset() {
    JSONObject jDatasetSchema = this.data.getJSONObject("schema");
    String datasetName = jDatasetSchema.getString("name");
    Boolean deleted = false;
    try {
      deleted = bigquery.delete(datasetName, DatasetDeleteOption.deleteContents());
    } catch (BigQueryException e) {
      logger.error("[DELETE_DATASET_ERROR]: " + e);
    }
    if (deleted) {
      logger.info("[DELETE_DATASET_SUCCESS]: " + datasetName);
    } else {
      logger.error("[DELETE_DATASET_ERROR]: " + datasetName);
    }
    return deleted;
  }

  public Table createTable() {
    JSONObject jTableSchema = this.data.getJSONObject("schema");
    String datasetName = jTableSchema.getString("dataset");
    String tableName = jTableSchema.getString("name");
    Table table = null;
    TableId tableId = TableId.of(datasetName, tableName);
    ArrayList<Field> fields = new ArrayList<Field>();
    fields = new SchemaConverter().toBQTableSchema(jTableSchema.getJSONArray("fields"));
    Schema schema = Schema.of(fields);
    TableDefinition tableDefinition = StandardTableDefinition.of(schema);
    TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
    try {
      table = bigquery.create(tableInfo);
      logger.info("[CREATE_TABLE_SUCCESS]: " + table);
    } catch (BigQueryException e) {
      logger.error("[CREATE_TABLE_ERROR]: " + e);
    }
    return table;
  }
  /*
  * For each table, raw requests (mostly one) are converted to BigQuery request objects
  */
  public InsertAllRequest prepareBigQueryInsertRequest(JSONObject data) {
    JSONObject jTableSchema = data.getJSONObject("schema");
    String datasetName = jTableSchema.getString("dataset");
    String tableName = jTableSchema.getString("name");
    TableId tableId = TableId.of(datasetName, tableName);
    Iterator<?> jRows = jTableSchema.getJSONArray("rows").iterator();
    JSONObject jRow = null;
    String insertId = null;
    InsertAllRequest.Builder rowBuilder =  InsertAllRequest.newBuilder(tableId);
    while (jRows.hasNext()) {
      jRow = (JSONObject) jRows.next();
      insertId = jRow.getString("insertId");
      Map<String, Object> row = jsonToMap(jRow.getJSONObject("json"));
      rowBuilder.addRow(insertId, row);
    }
    return rowBuilder.build();
  }
  /*
  * For each table, accumulated raw insert requests are converted to BigQuery request objects
  */
  public static InsertAllRequest prepareBigQueryInsertRequestFromBuffer(ArrayList<JSONObject> bufferedRequests) {
    JSONObject jTableSchema = bufferedRequests.get(0).getJSONObject("schema");
    String datasetName = jTableSchema.getString("dataset");
    String tableName = jTableSchema.getString("name");
    TableId tableId = TableId.of(datasetName, tableName);
    InsertAllRequest.Builder rowBuilder =  InsertAllRequest.newBuilder(tableId);
    JSONObject jRow = null;
    String insertId = null;
    for(JSONObject bufferedRequest : bufferedRequests){
      Iterator<?> jRows = bufferedRequest.getJSONObject("schema").getJSONArray("rows").iterator();;
      while (jRows.hasNext()) {
        jRow = (JSONObject) jRows.next();
        insertId = jRow.getString("insertId");
        Map<String, Object> row = jsonToMap(jRow.getJSONObject("json"));
        rowBuilder.addRow(insertId, row);
      }
    }
    rowBuilder.setSkipInvalidRows(true);
    return rowBuilder.build();
  }
  public static ArrayList<InsertAllResponse> makeInsertApiCall(InsertAllRequest request,ArrayList<InsertAllResponse> responses){
    try{
      InsertAllResponse response = bigquery.insertAll(request);
      if (response.hasErrors()) {
        logger.error("Error inserting data: " + response);
      }else {
        logger.info("Inserted : " + response);
      }
      responses.add(response);
    }catch(BigQueryException e){
      logger.error("[INSERT_TABLE_ERROR]: " + e);
      try{
        InsertAllResponse response = bigquery.insertAll(request);
        if (response.hasErrors()) {
          logger.error("Error inserting data: " + response);
        }else {
          logger.info("Inserted : " + response);
        }
        responses.add(response);
      }catch(BigQueryException ex){
        logger.error("[INSERT_TABLE_ERROR]: " + ex);
      }
    }
    return responses;
  }
  public static ArrayList<InsertAllResponse> dispatchBatchInsertionsBasedOnSize(BatchInsertionControl insertionControl){
    HashMap<String, HashMap<String,BlockingQueue<JSONObject>>>  bufferedRequests = insertionControl.getBufferedRequests();
    ArrayList<InsertAllResponse> responses =  new ArrayList<>();
    for (String dataset : bufferedRequests.keySet()) {
      for (String table : bufferedRequests.get(dataset).keySet()){
        Integer readyBufferedRequests = bufferedRequests.get(dataset).get(table).size();
        Integer tableLevelBatchSize = insertionControl.getBatchSize();
        Integer batches = readyBufferedRequests/tableLevelBatchSize;
        for (int i=0; i< batches + 1 ; i++ ) {
          ArrayList<JSONObject> rawInsertRequests = multipop(bufferedRequests.get(dataset).get(table),tableLevelBatchSize);
          if(!rawInsertRequests.isEmpty()){
            InsertAllRequest bqInsertRequests = prepareBigQueryInsertRequestFromBuffer(rawInsertRequests);
            responses = makeInsertApiCall(bqInsertRequests,responses);
          }
        }
      }
    }
    return responses;
  }
  public static ArrayList<InsertAllResponse> dispatchSingleBatchInsertion(BatchInsertionControl insertionControl){
    HashMap<String, HashMap<String,BlockingQueue<JSONObject>>>  bufferedRequests = insertionControl.getBufferedRequests();
    ArrayList<InsertAllResponse> responses =  new ArrayList<>();
    for (String dataset : bufferedRequests.keySet()) {
      for (String table : bufferedRequests.get(dataset).keySet()){
        Integer tableLevelBatchSize = insertionControl.getBatchSize();
        ArrayList<JSONObject> rawInsertRequests = multipop(bufferedRequests.get(dataset).get(table),tableLevelBatchSize);//bufferedRequests.get(dataset).get(table);
        if(!rawInsertRequests.isEmpty()){
          InsertAllRequest bqInsertRequests = prepareBigQueryInsertRequestFromBuffer(rawInsertRequests);
          System.out.println("Insert api call for ("+ Integer.toString(tableLevelBatchSize) +")");
          responses = makeInsertApiCall(bqInsertRequests,responses);
        }
      }
    }
    return responses;
  }
  private static ArrayList<JSONObject> multipop(BlockingQueue<JSONObject>requestList, Integer size ){
    BlockingQueue<JSONObject> q = requestList;
    ArrayList<JSONObject> popedRequests = new ArrayList<>();
    Integer count = 0;
    while(count < size && q.peek() != null){
      try{
        popedRequests.add(q.take());
      }catch(InterruptedException e){
        e.printStackTrace();
      }
      count++;
    }
    return popedRequests;
  }
  /*
  * Makes Api call for batch of raw events from rabbitmq.
  * Cached Requests are maintained at BatchInsertionControl [refer].
  * Retuens list of responses for each BigQuery table batch insertion
  */
  public ArrayList<InsertAllResponse> insert(BatchInsertionControl insertionControl){
    try{
      insertionControl.buffer(this.data);
    }catch(Exception e){
      logger.error("Error inserting data: "+ e);
      // this.insertNoBffer(); //direct api call
    }
    return null;
  }
  /*
  * Single Api call for each raw event from rabbitmq
  */
  public InsertAllResponse insertNoBffer() {
    InsertAllRequest request = this.prepareBigQueryInsertRequest(this.data);
    InsertAllResponse response = bigquery.insertAll(request);
    if (response.hasErrors()) {
      logger.error("Error inserting data: " + response);
    }else {
      logger.info("Inserted : " + response);
    }
    return response;
  }

  public Table updateTable() {
    JSONObject jTableSchema = this.data.getJSONObject("schema");
    String datasetName = jTableSchema.getString("dataset");
    String tableName = jTableSchema.getString("name");
    String newFriendlyName = jTableSchema.getString("newFriendlyName");
    Table newTable = null;
    Table oldTable = bigquery.getTable(datasetName, tableName);
    TableInfo tableInfo = oldTable.toBuilder().setFriendlyName(newFriendlyName).build();
    try {
      newTable = bigquery.update(tableInfo);
      logger.info("[UPDATE_TABLE_SUCCESS]: " + newTable);
    } catch (BigQueryException e) {
      logger.error("[UPDATE_TABLE_ERROR]: " + e);
    }
    return newTable;
  }

  public Boolean deleteTable() {
    JSONObject jTableSchema = this.data.getJSONObject("schema");
    String datasetName = jTableSchema.getString("dataset");
    String tableName = jTableSchema.getString("name");
    Boolean deleted = false;
    try {
      deleted = bigquery.delete(datasetName, tableName);
    } catch (BigQueryException e) {
      logger.error("[DELETE_TABLE_ERROR]: datasetName: " + datasetName + " tableName: " + tableName);
    }
    if (deleted) {
      logger.info("[DELETE_TABLE_SUCCESS]: datasetName: " + datasetName + " tableName: " + tableName);
    } else {
    }
    return deleted;
  }

  private static Map<String, Object> jsonToMap(JSONObject json) throws JSONException {
      Map<String, Object> retMap = new HashMap<String, Object>();

      if(json != JSONObject.NULL) {
          retMap = toMap(json);
      }
      return retMap;
  }

  private static Map<String, Object> toMap(JSONObject object) throws JSONException {
      Map<String, Object> map = new HashMap<String, Object>();

      Iterator<String> keysItr = object.keys();
      while(keysItr.hasNext()) {
          String key = keysItr.next();
          Object value = object.get(key);

          if(value instanceof JSONArray) {
              value = toList((JSONArray) value);
          }

          else if(value instanceof JSONObject) {
              value = toMap((JSONObject) value);
          }
          map.put(key, value);
      }
      return map;
  }

  private static List<Object> toList(JSONArray array) throws JSONException {
      List<Object> list = new ArrayList<Object>();
      for(int i = 0; i < array.length(); i++) {
          Object value = array.get(i);
          if(value instanceof JSONArray) {
              value = toList((JSONArray) value);
          }

          else if(value instanceof JSONObject) {
              value = toMap((JSONObject) value);
          }
          list.add(value);
      }
      return list;
  }
}
