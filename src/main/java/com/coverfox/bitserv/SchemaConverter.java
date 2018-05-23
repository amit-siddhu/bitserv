package com.coverfox.bitserv;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Field.Builder;
import java.util.ArrayList;
import java.util.Iterator;

import org.json.JSONArray;
import org.json.JSONObject;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SchemaConverter {

  private static final Logger logger = LogManager.getLogger(SchemaConverter.class);
  public ArrayList<Field> toBQTableSchema(JSONArray jsonSchema) {

    ArrayList<Field> bqFields = new ArrayList<Field>();
    Iterator<?> jFields = jsonSchema.iterator();

    JSONObject jField = null;
    String type = null;

    while (jFields.hasNext()) {
      jField = (JSONObject) jFields.next();
      type = jField.getString("type");
      switch (type) {
        case "boolean":
          bqFields.add(fieldHelper(jField, Field.Type.bool()).build());
          break;
        case "bytes":
          bqFields.add(fieldHelper(jField, Field.Type.bytes()).build());
          break;
        case "date":
          bqFields.add(fieldHelper(jField, Field.Type.date()).build());
          break;
        case "datetime":
          bqFields.add(fieldHelper(jField, Field.Type.datetime()).build());
          break;
        case "float":
          bqFields.add(fieldHelper(jField, Field.Type.floatingPoint()).build());
          break;
        case "integer":
          bqFields.add(fieldHelper(jField, Field.Type.integer()).build());
          break;
        case "record":
          bqFields.add(fieldHelper(jField, Field.Type.record(new SchemaConverter().toBQTableSchema(jField.getJSONArray("fields")))).build());
          break;
        case "string":
          bqFields.add(fieldHelper(jField, Field.Type.string()).build());
          break;
        case "time":
          bqFields.add(fieldHelper(jField, Field.Type.time()).build());
          break;
        case "timestamp":
          bqFields.add(fieldHelper(jField, Field.Type.timestamp()).build());
          break;
        default:
          logger.error("Invalid field type: [" + type + "] schema: " + jsonSchema);
          return null;
      }
    }
    return bqFields;
  }

  private static Builder fieldHelper(JSONObject field, Field.Type type) {
    Builder curField = Field.newBuilder(field.getString("name"), type);
    if (field.has("mode")) {
      switch (field.getString("mode")) {
        case "nullable":
          curField.setMode(Field.Mode.NULLABLE);
          break;
        case "repeated":
          curField.setMode(Field.Mode.REPEATED);
          break;
        case "required":
          curField.setMode(Field.Mode.REQUIRED);
          break;
        default:
          logger.error("Invalid mode: [" + field.getString("mode") + "] on field: " + field);
          return null;
      }
    }
    if (field.has("description")) {
      curField.setDescription(field.getString("description"));
    }
    return curField;
  }
}
