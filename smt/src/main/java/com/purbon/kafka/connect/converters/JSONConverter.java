package com.purbon.kafka.connect.converters;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.connect.storage.ConverterType;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.apache.kafka.connect.storage.StringConverterConfig;

public class JSONConverter implements Converter, HeaderConverter {

  private JsonConverterConfig config;
  private JSONUtils utils;
  private DataTypeConverter converter;

  @Override
  public void close() {
    //empty
  }

  @Override
  public void configure(Map<String, ?> configs) {
    this.config = new JsonConverterConfig(configs);
    this.utils = new JSONUtils(config);
    this.converter = new DataTypeConverter(config);
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    Map<String, Object> conf = new HashMap<>(configs);
    conf.put(StringConverterConfig.TYPE_CONFIG, isKey ? ConverterType.KEY.getName() : ConverterType.VALUE.getName());
    configure(conf);
  }

  @Override
  public byte[] fromConnectData(String topic, Schema schema, Object value) {

    String jsonString = null;
    try {
      jsonString = JSONUtils.toString(value);
    } catch (JsonProcessingException e) {
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
      schema = utils.buildSchemaFromJSONString(jsonString);
    } catch (JsonProcessingException e) {
      e.printStackTrace();
      throw new DataException("json with incorrect format");
    }

    try {
      return new SchemaAndValue(schema,
          converter.convertToConnect(schema, JSONUtils.toJsonNode(jsonString)));
    } catch (JsonProcessingException e) {
      e.printStackTrace();
      throw new DataException("json with incorrect format");
    }
  }

  @Override
  public SchemaAndValue toConnectHeader(String topic, String headerKey, byte[] value) {
    return toConnectData(topic, value);
  }

  @Override
  public byte[] fromConnectHeader(String topic, String headerKey, Schema schema, Object value) {
    return fromConnectData(topic, schema, value);
  }

  @Override
  public ConfigDef config() {
    return JsonConverterConfig.configDef();
  }

}
