package com.coverfox.bitserv;

import com.google.api.client.util.Charsets;
import com.google.api.gax.paging.Page;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.BigQuery.DatasetDeleteOption;
import com.google.cloud.bigquery.BigQuery.DatasetListOption;
import com.google.cloud.bigquery.BigQuery.JobListOption;
import com.google.cloud.bigquery.BigQuery.TableDataListOption;
import com.google.cloud.bigquery.BigQuery.TableListOption;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobConfiguration;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobStatistics.LoadStatistics;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryParameterValue;
import com.google.cloud.bigquery.QueryRequest;
import com.google.cloud.bigquery.QueryResponse;
import com.google.cloud.bigquery.QueryResult;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDataWriteChannel;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.WriteChannelConfiguration;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.concurrent.TimeoutException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;


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

  private Dataset getDataset(String datasetName) {
    Dataset dataset = null;
    try {
      dataset = bigquery.getDataset(datasetName);
      logger.info("[GET_DATASET_SUCCESS]: " + dataset);
    } catch (BigQueryException e) {
      logger.error("[GET_DATASET_ERROR]: " + dataset);
    }
    return dataset;
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

  public InsertAllResponse insertAll() {
    JSONObject jTableSchema = this.data.getJSONObject("schema");
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

    InsertAllResponse response = bigquery.insertAll(rowBuilder.build());

    if (response.hasErrors()) {
      logger.error("Error inserting data: " + response);
    }
    logger.info("Inserted : " + jTableSchema.getJSONArray("rows"));
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
