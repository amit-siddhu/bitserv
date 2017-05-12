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

import java.util.UUID;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeoutException;


public class BigQueryOps {

  private static final Logger logger = LogManager.getLogger(BigQueryOps.class);
  public static final BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

  public static Dataset createDataset(String datasetName) {
    Dataset dataset = null;
    DatasetInfo datasetInfo = DatasetInfo.newBuilder(datasetName).build();
    try {
      dataset = bigquery.create(datasetInfo);
      logger.info("[CREATE_DATASET_SUCCESS]: " + dataset);
    } catch (BigQueryException e) {
      logger.error("[CREATE_DATASET_ERROR]: " + e);
    }
    return dataset;
  }

  public static Dataset updateDataset(String datasetName, String newFriendlyName) {
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

  public Dataset getDataset(String datasetName) {
    Dataset dataset = null;
    try {
      dataset = bigquery.getDataset(datasetName);
      logger.info("[GET_DATASET_SUCCESS]: " + dataset);
    } catch (BigQueryException e) {
      logger.error("[GET_DATASET_ERROR]: " + dataset);
    }
    return dataset;
  }

  public Dataset getDatasetFromId(String projectId, String datasetName) {
    Dataset dataset = null;
    DatasetId datasetId = DatasetId.of(projectId, datasetName);
    try {
      dataset = bigquery.getDataset(datasetId);
      logger.info("[GET_DATASET_SUCCESS]: " + dataset);
    } catch (BigQueryException e) {
      logger.error("[GET_DATASET_ERROR]: " + dataset);
    }
    return dataset;
  }

  public static Boolean deleteDataset(String datasetName) {
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

  public Boolean deleteDatasetFromId(String projectId, String datasetName) {
    Boolean deleted = false;
    DatasetId datasetId = DatasetId.of(projectId, datasetName);
    try {
      deleted = bigquery.delete(datasetId, DatasetDeleteOption.deleteContents());
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

  public boolean doesDatasetExist(Dataset dataset) {
    boolean exists = false;
    exists = dataset.exists();
    if (exists) {
      logger.info("[DATASET_EXIST]: " + dataset);
    } else {
      logger.error("[DATASET_NOT_FOUND]: " + dataset);
    }
    return exists;
  }

  public Dataset reloadDataset(Dataset dataset) {
    Dataset latestDataset = dataset.reload();
    if (latestDataset == null) {
      logger.error("[RELOAD_DATASET_FAILED]: " + dataset);
    }
    return latestDataset;
  }

  public static Table createTable(String datasetName, String tableName, String fieldName) {
    Table table = null;
    TableId tableId = TableId.of(datasetName, tableName);
    Field field = Field.of(fieldName, Field.Type.string());
    Schema schema = Schema.of(field);
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

  public Table getTable(String datasetName, String tableName) {
    Table table = null;
    try {
      table = bigquery.getTable(datasetName, tableName);
      logger.info("[GET_TABLE_SUCCESS]: " + table);
    } catch (BigQueryException e) {
      logger.info("[GET_TABLE_ERROR]: " + table);
    }
    return table;
  }

  public Table getTableFromId(String projectId, String datasetName, String tableName) {
    Table table = null;
    TableId tableId = TableId.of(projectId, datasetName, tableName);
    try {
      table = bigquery.getTable(datasetName, tableName);
      logger.info("[GET_TABLE_SUCCESS]: " + table);
    } catch (BigQueryException e) {
      logger.info("[GET_TABLE_ERROR]: " + table);
    }
    return table;
  }

  public long writeToTable(String datasetName, String tableName, String csvData)
      throws IOException, InterruptedException, TimeoutException {
    TableId tableId = TableId.of(datasetName, tableName);
    WriteChannelConfiguration writeChannelConfiguration =
        WriteChannelConfiguration.newBuilder(tableId)
            .setFormatOptions(FormatOptions.csv())
            .build();
    TableDataWriteChannel writer = bigquery.writer(writeChannelConfiguration);
     try {
        writer.write(ByteBuffer.wrap(csvData.getBytes(Charsets.UTF_8)));
      } finally {
        writer.close();
      }
      Job job = writer.getJob();
      job = job.waitFor();
      LoadStatistics stats = job.getStatistics();
      return stats.getOutputRows();
  }

  public long writeFileToTable(String datasetName, String tableName, Path csvPath)
      throws IOException, InterruptedException, TimeoutException {
    TableId tableId = TableId.of(datasetName, tableName);
    WriteChannelConfiguration writeChannelConfiguration =
        WriteChannelConfiguration.newBuilder(tableId)
            .setFormatOptions(FormatOptions.csv())
            .build();
    TableDataWriteChannel writer = bigquery.writer(writeChannelConfiguration);
    try (OutputStream stream = Channels.newOutputStream(writer)) {
      Files.copy(csvPath, stream);
    }
    Job job = writer.getJob();
    job = job.waitFor();
    LoadStatistics stats = job.getStatistics();
    return stats.getOutputRows();
  }

  public static InsertAllResponse insertAll(String datasetName, String tableName) {
    TableId tableId = TableId.of(datasetName, tableName);
    // Values of the row to insert
    Map<String, Object> rowContent = new HashMap<>();
    rowContent.put("booleanField", true);
    // Bytes are passed in base64
    rowContent.put("bytesField", "Cg0NDg0="); // 0xA, 0xD, 0xD, 0xE, 0xD in base64
    // Records are passed as a map
    Map<String, Object> recordsContent = new HashMap<>();
    recordsContent.put("stringField", "Hello, World!");
    rowContent.put("recordField", recordsContent);
    InsertAllResponse response = bigquery.insertAll(InsertAllRequest.newBuilder(tableId)
        .addRow("rowId", rowContent)
        // More rows can be added in the same RPC by invoking .addRow() on the builder
        .build());
    if (response.hasErrors()) {
      // If any of the insertions failed, this lets you inspect the errors
      for (Entry<Long, List<BigQueryError>> entry : response.getInsertErrors().entrySet()) {
        // inspect row error
      }
    }
    return response;
  }

  public static Table updateTable(String datasetName, String tableName, String newFriendlyName) {
    Table newTable = null;
    Table oldTable = bigquery.getTable(datasetName, tableName);
    TableInfo tableInfo = oldTable.toBuilder().setFriendlyName(newFriendlyName).build();
    try {
      newTable = bigquery.update(tableInfo);
    } catch (BigQueryException e) {
      logger.error("[UPDATE_TABLE_ERROR]: " + e);
    }
    return newTable;
  }

  public Boolean deleteTable(String datasetName, String tableName) {
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

  public Boolean deleteTableFromId(String projectId, String datasetName, String tableName) {
    Boolean deleted = false;
    TableId tableId = TableId.of(projectId, datasetName, tableName);
    try {
      deleted = bigquery.delete(tableId);
    } catch (BigQueryException e) {
      logger.error("[DELETE_TABLE_ERROR]: datasetName: " + datasetName + " tableName: " + tableName);
    }
    if (deleted) {
      logger.info("[DELETE_TABLE_SUCCESS]: datasetName: " + datasetName + " tableName: " + tableName);
    } else {
    }
    return deleted;
  }

  public Page<List<FieldValue>> listTableData(String datasetName, String tableName) {
    Page<List<FieldValue>> tableData =
        bigquery.listTableData(datasetName, tableName, TableDataListOption.pageSize(100));
    for (List<FieldValue> row : tableData.iterateAll()) {
      // do something with the row
    }
    return tableData;
  }

  public Page<List<FieldValue>> listTableDataFromId(String datasetName, String tableName) {
    TableId tableIdObject = TableId.of(datasetName, tableName);
    Page<List<FieldValue>> tableData =
        bigquery.listTableData(tableIdObject, TableDataListOption.pageSize(100));
    for (List<FieldValue> row : tableData.iterateAll()) {
      // do something with the row
    }
    return tableData;
  }

  public Job createJob(String query) {
    Job job = null;
    JobConfiguration jobConfiguration = QueryJobConfiguration.of(query);
    JobInfo jobInfo = JobInfo.of(jobConfiguration);
    try {
      job = bigquery.create(jobInfo);
      logger.info("[CREATE_JOB_SUCCESS]: " + job);
    } catch (BigQueryException e) {
      logger.error("[CREATE_JOB_ERROR]: " + job);
    }
    return job;
  }

  public Page<Job> listJobs() {
    Page<Job> jobs = bigquery.listJobs(JobListOption.pageSize(100));
    for (Job job : jobs.iterateAll()) {
      // do something with the job
    }
    return jobs;
  }

  public Job getJob(String jobName) {
    Job job = null;
    try {
      job = bigquery.getJob(jobName);
      logger.info("[GET_JOB_SUCCESS]: " + job);
    } catch (BigQueryException e) {
      logger.error("[GET_JOB_ERROR]: " + job);
    }
    if (job == null) {
      logger.error("[GET_JOB_FAILED]: " + job);
    }
    return job;
  }

  public Job getJobFromId(String jobName) {
    Job job = null;
    JobId jobIdObject = JobId.of(jobName);
    try {
      job = bigquery.getJob(jobIdObject);
      logger.info("[GET_JOB_SUCCESS]: " + job);
    } catch (BigQueryException e) {
      logger.error("[GET_JOB_ERROR]: " + e);
    }
    if (job == null) {
      logger.error("[GET_JOB_FAILED]: " + job);
    }
    return job;
  }

  public boolean cancelJob(String jobName) {
    boolean success = false;
    try {
      success = bigquery.cancel(jobName);
    } catch (BigQueryException e) {
      logger.error("[CANCEL_JOB_ERROR]: " + e);
    }
    if (success) {
      logger.info("[CANCEL_JOB_SUCCESS]: " + jobName);
    } else {
      logger.error("[CANCEL_JOB_FAILED]: " + jobName);
    }
    return success;
  }

  public boolean cancelJobFromId(String jobName) {
    boolean success = false;
    JobId jobId = JobId.of(jobName);
    try {
      success = bigquery.cancel(jobId);
    } catch (BigQueryException e) {
      logger.error("[CANCEL_JOB_ERROR]: " + e);
    }
    if (success) {
      logger.info("[CANCEL_JOB_SUCCESS]: " + jobName);
    } else {
      logger.error("[CANCEL_JOB_FAILED]: " + jobName);
    }
    return success;
  }

  public QueryResponse runQuery(String query) throws InterruptedException {
    QueryRequest request = QueryRequest.of(query);
    QueryResponse response = bigquery.query(request);
    while (!response.jobCompleted()) {
      Thread.sleep(1000);
      response = bigquery.getQueryResults(response.getJobId());
    }
    if (response.hasErrors()) {
      logger.error("[RUN_QUERY_ERROR]: " + response);
    }
    QueryResult result = response.getResult();
    for (List<FieldValue> row : result.iterateAll()) {
      // do something with the data
    }
    return response;
  }

  public QueryResponse runQueryWithParameters(String query) throws InterruptedException {
    QueryRequest request = QueryRequest.newBuilder(query)
        .setUseLegacySql(false)
        .addNamedParameter("wordCount", QueryParameterValue.int64(5))
        .build();
    QueryResponse response = bigquery.query(request);
    while (!response.jobCompleted()) {
      Thread.sleep(1000);
      response = bigquery.getQueryResults(response.getJobId());
    }
    if (response.hasErrors()) {
      logger.error("[RUN_QUERY_ERROR]: " + response);
    }
    QueryResult result = response.getResult();
    for (List<FieldValue> row : result.iterateAll()) {
      // do something with the data
    }
    return response;
  }

  public QueryResponse queryResults(final String query) throws InterruptedException {
    QueryRequest request = QueryRequest.of(query);
    QueryResponse response = bigquery.query(request);
    while (!response.jobCompleted()) {
      Thread.sleep(1000);
      response = bigquery.getQueryResults(response.getJobId());
    }
    if (response.hasErrors()) {
      logger.error("[GET_QUERY_RESULTS_ERROR]: " + response);
    }
    QueryResult result = response.getResult();
    for (List<FieldValue> row : result.iterateAll()) {
      // do something with the data
    }
    return response;
  }

  public void getData(String data) throws Exception {

    QueryJobConfiguration queryConfig =
        QueryJobConfiguration.newBuilder(
                "SELECT "
                    + "APPROX_TOP_COUNT(corpus, 10) as title, "
                    + "COUNT(*) as unique_words "
                    + "FROM `publicdata.samples." + data + "`;")
            .setUseLegacySql(false)
            .build();

    JobId jobId = JobId.of(UUID.randomUUID().toString());
    Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

    queryJob = queryJob.waitFor();

    if (queryJob == null) {
      throw new RuntimeException("Job no longer exists");
    } else if (queryJob.getStatus().getError() != null) {

      throw new RuntimeException(queryJob.getStatus().getError().toString());
    }

    QueryResponse response = bigquery.getQueryResults(jobId);

    QueryResult result = response.getResult();

    while (result != null) {
      for (List<FieldValue> row : result.iterateAll()) {
        List<FieldValue> titles = row.get(0).getRepeatedValue();
        System.out.println("titles:");

        for (FieldValue titleValue : titles) {
          List<FieldValue> titleRecord = titleValue.getRecordValue();
          String title = titleRecord.get(0).getStringValue();
          long uniqueWords = titleRecord.get(1).getLongValue();
          System.out.printf("\t%s: %d\n", title, uniqueWords);
        }

        long uniqueWords = row.get(1).getLongValue();
        System.out.printf("total unique words: %d\n", uniqueWords);
      }

      result = result.getNextPage();
    }
  }
}
