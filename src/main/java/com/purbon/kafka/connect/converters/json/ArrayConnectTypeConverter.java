package com.purbon.kafka.connect.converters.json;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import org.apache.kafka.connect.data.Schema;

public class ArrayConnectTypeConverter extends AbstractConnectTypeConverter {

  @Override
  public Object convert(Schema schema, JsonNode value) {
    Schema elemSchema = schema == null ? null : schema.valueSchema();
    ArrayList<Object> result = new ArrayList<>();
    for (JsonNode elem : value) {
      result.add(convertToConnect(elemSchema, elem));
    }
    return result;
  }
}
