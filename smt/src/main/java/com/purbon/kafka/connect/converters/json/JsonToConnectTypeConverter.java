package com.purbon.kafka.connect.converters.json;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.connect.data.Schema;

public interface JsonToConnectTypeConverter {
  Object convert(Schema schema, JsonNode value);
}
