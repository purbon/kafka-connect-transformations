package com.purbon.kafka.connect.converters.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.purbon.kafka.connect.converters.JsonConverterConfig;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.errors.DataException;

public class TimeLogicalTypeConverter implements LogicalTypeConverter {
  @Override
  public JsonNode toJson(final Schema schema, final Object value, final JsonConverterConfig config) {
    if (!(value instanceof java.util.Date))
      throw new DataException("Invalid type for Time, expected Date but was " + value.getClass());
    return JsonNodeFactory.instance.numberNode(Time.fromLogical(schema, (java.util.Date) value));
  }

  @Override
  public Object toConnect(final Schema schema, final JsonNode value) {
    if (!(value.isInt()))
      throw new DataException("Invalid type for Time, underlying representation should be integer but was " + value.getNodeType());
    return Time.toLogical(schema, value.intValue());
  }
}
