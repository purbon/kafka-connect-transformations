package com.purbon.kafka.connect.converters.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.purbon.kafka.connect.converters.JsonConverterConfig;
import org.apache.kafka.connect.data.Schema;

public interface LogicalTypeConverter {
  JsonNode toJson(Schema schema, Object value, JsonConverterConfig config);
  Object toConnect(Schema schema, JsonNode value);
}
