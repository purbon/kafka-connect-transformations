package com.purbon.kafka.connect.converters.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.purbon.kafka.connect.converters.DataTypeConverter;
import java.util.ArrayList;
import org.apache.kafka.connect.data.Schema;

public class ArrayConnectTypeConverter implements JsonToConnectTypeConverter {

  private final DataTypeConverter dataTypeConverter;

  public ArrayConnectTypeConverter(DataTypeConverter dataTypeConverter) {
    this.dataTypeConverter = dataTypeConverter;
  }

  @Override
  public Object convert(Schema schema, JsonNode value) {
    Schema elemSchema = schema == null ? null : schema.valueSchema();
    ArrayList<Object> result = new ArrayList<>();
    for (JsonNode elem : value) {
      result.add(dataTypeConverter.convertToConnect(elemSchema, elem));
    }
    return result;
  }
}
