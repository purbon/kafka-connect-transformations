package com.purbon.kafka.connect.converters;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;

public class JSONUtils {


  private final JsonConverterConfig config;
  private static ObjectMapper mapper = new ObjectMapper();

  public JSONUtils(JsonConverterConfig config) {
    this.config = config;
  }

  public static String toString(Object value) throws JsonProcessingException {
    return mapper.writeValueAsString(value);
  }

  public static JsonNode toJsonNode(String value) throws JsonProcessingException {
    return mapper.readTree(value);
  }

  public Schema buildSchemaFromJSONString(String jsonString) throws JsonProcessingException {
    TypeReference<HashMap<Object, Object>> typeRef = new TypeReference<HashMap<Object, Object>>() {
    };

    Map<Object, Object> object = mapper.readValue(jsonString, typeRef);

    SchemaBuilder struct = SchemaBuilder.struct();
    for (Object key : object.keySet()) {
      Object value = object.get(key);
      if (value != null) {
        Schema schema = buildSchema(key.toString(), value);
        struct.field(key.toString(), schema);
      }
    }

    return struct.build();
  }

  public Schema buildSchema(String key, Object value) {
    Schema schema;
    if (value instanceof Integer) {
      schema = Schema.INT64_SCHEMA;
    } else if (value instanceof Boolean) {
      schema = Schema.BOOLEAN_SCHEMA;
    } else if (value instanceof Double) {
      schema = Schema.FLOAT64_SCHEMA;
    } else if (value instanceof String) {
      String valueAsString = String.valueOf(value);
      schema = detectDataTypeWithinString(key, valueAsString);
    } else if (value instanceof ArrayList) {
      List list = (ArrayList) value;
      schema = SchemaBuilder
          .array(buildSchema(key, list.get(0)))
          .build();
    } else if (value instanceof HashMap) {
      SchemaBuilder builder = SchemaBuilder.struct();
      Map<String, Object> map = (Map) value;
      for (String myKey : map.keySet()) {
        builder.field(myKey, buildSchema(key, map.get(myKey)));
      }
      schema = builder.build();
    } else {
      throw new DataException(("wrong value detection of " + value));
    }
    return schema;
  }

  public Schema detectDataTypeWithinString(String key, String value) {
    Schema schema;
    if (isStringADate(key, value)) {
      schema = SchemaBuilder
          .string()
          .name(Date.LOGICAL_NAME)
          .build();
    } else if (isStringATimestamp(key, value)) {
      schema = SchemaBuilder
          .string()
          .name(Timestamp.LOGICAL_NAME)
          .build();
    } else if (isStringAnInteger(value)) {
      schema = Schema.INT64_SCHEMA;
    } else if (isStringAFloat(value)) {
      schema = Schema.FLOAT64_SCHEMA;
    } else {
      schema = SchemaBuilder
          .string()
          .defaultValue("")
          .optional()
          .build();
    }
    return schema;
  }

  private boolean isStringAnInteger(String value) {
    try {
      Integer.valueOf(value);
      return true;
    } catch (Exception ex){
      return false;
    }
  }

  private  boolean isStringAFloat(String value) {
    try {
      Float.valueOf(value);
      return true;
    } catch (Exception ex){
      return false;
    }
  }

  private boolean isStringADate(String key, String value) {
    List<String> dateAttributes = config.getDateAttributes();
    if (key.toLowerCase().endsWith("_dt") || dateAttributes.contains(key)) {
      return true;
    } else {
      return false;
    }
  }

  private boolean isStringATimestamp(String key, String value) {
    List<String> timestampAttributes = config.getTimestampAttributes();
    if (key.toLowerCase().endsWith("_ts") || timestampAttributes.contains(key)) {
      return true;
    } else {
      return false;
    }
  }
}
