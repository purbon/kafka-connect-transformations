package com.purbon.kafka.connect.converters;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.beans.Encoder;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;

public class JSONConverter implements Converter, HeaderConverter {

  ObjectMapper mapper = new ObjectMapper();

  @Override
  public void close() throws IOException {
    //empty
  }

  @Override
  public void configure(Map<String, ?> configs) {
    //TODO process config
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    configure(configs);
  }

  @Override
  public byte[] fromConnectData(String topic, Schema schema, Object value) {

    String jsonString = null;
    try {
      jsonString = mapper.writeValueAsString(value);
    } catch (JsonProcessingException e) {
      e.printStackTrace();
      throw new DataException("Json with incorrect format");
    }
    return Base64.getEncoder().encode(jsonString.getBytes());
  }

  @Override
  public SchemaAndValue toConnectData(String topic, byte[] value) {

    // This handles a tombstone message
    if (value == null) {
      return SchemaAndValue.NULL;
    }

    String jsonString = new String(value);
    Schema schema = null;
    try {
      schema = inspectJSONSchema(jsonString);
    } catch (JsonProcessingException e) {
      e.printStackTrace();
      throw new DataException("json with incorrect format");
    }
    return new SchemaAndValue(schema, jsonString);
  }

  private Schema inspectJSONSchema(String jsonString) throws JsonProcessingException {
    TypeReference<HashMap<Object, Object>> typeRef = new TypeReference<HashMap<Object, Object >>() {};

    Map<Object, Object>  object = mapper.readValue(jsonString, typeRef);

    SchemaBuilder struct = SchemaBuilder.struct() ;
    for(Object key : object.keySet()) {
      Object value = object.get(key);
      Schema schema = buildSchema(key.toString(), value);
      struct.field(key.toString(), schema);
    }

   return struct.build();
  }

  private Schema buildSchema(String key, Object value) {
    Schema schema;
    if (value instanceof Integer) {
      schema = Schema.INT64_SCHEMA;
    } else if ( value instanceof Boolean) {
      schema = Schema.BOOLEAN_SCHEMA;
    } else if (value instanceof Double) {
      schema = Schema.FLOAT64_SCHEMA;
    } else if (value instanceof String) {
      schema = Schema.STRING_SCHEMA;
    } else if (value instanceof ArrayList) {
      List list = (ArrayList)value;
      schema = SchemaBuilder
          .array(buildSchema(key, list.get(0)))
          .build();
    } else if (value instanceof HashMap) {
      SchemaBuilder builder = SchemaBuilder.struct();
      Map<String, Object> map = (Map)value;
      for(String myKey: map.keySet()) {
        builder.field(myKey, buildSchema(key, map.get(myKey)));
      }
      schema = builder.build();
    }
    else {
      throw new DataException(("wrong value detection"));
    }
    return schema;
  }

  @Override
  public SchemaAndValue toConnectHeader(String topic, String headerKey, byte[] value) {
    return toConnectData(topic, value);
  }

  @Override
  public byte[] fromConnectHeader(String topic, String headerKey, Schema schema, Object value) {
    return fromConnectData(topic, schema, value);
  }

  private JsonConverterConfig config;


  @Override
  public ConfigDef config() {
    return JsonConverterConfig.configDef();
  }
}
