package com.purbon.kafka.connect.converters.json;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;

public class MapConnectTypeConverter extends AbstractConnectTypeConverter {

  @Override
  public Object convert(Schema schema, JsonNode value) {
    Schema keySchema = schema == null ? null : schema.keySchema();
    Schema valueSchema = schema == null ? null : schema.valueSchema();

    // If the map uses strings for keys, it should be encoded in the natural JSON format. If it uses other
    // primitive types or a complex type as a key, it will be encoded as a list of pairs. If we don't have a
    // schema, we default to encoding in a Map.
    Map<Object, Object> result = new HashMap<>();
    if (schema == null || keySchema.type() == Schema.Type.STRING) {
      if (!value.isObject())
        throw new DataException("Maps with string fields should be encoded as JSON objects, but found " + value.getNodeType());
      Iterator<Entry<String, JsonNode>> fieldIt = value.fields();
      while (fieldIt.hasNext()) {
        Map.Entry<String, JsonNode> entry = fieldIt.next();
        result.put(entry.getKey(), convertToConnect(valueSchema, entry.getValue()));
      }
    } else {
      if (!value.isArray())
        throw new DataException("Maps with non-string fields should be encoded as JSON array of tuples, but found " + value.getNodeType());
      for (JsonNode entry : value) {
        if (!entry.isArray())
          throw new DataException("Found invalid map entry instead of array tuple: " + entry.getNodeType());
        if (entry.size() != 2)
          throw new DataException("Found invalid map entry, expected length 2 but found :" + entry.size());
        result.put(convertToConnect(keySchema, entry.get(0)),
            convertToConnect(valueSchema, entry.get(1)));
      }
    }
    return result;
  }
}
