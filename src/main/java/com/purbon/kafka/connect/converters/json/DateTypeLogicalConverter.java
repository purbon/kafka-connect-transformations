package com.purbon.kafka.connect.converters.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.purbon.kafka.connect.converters.JsonConverterConfig;
import java.text.SimpleDateFormat;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;

public class DateTypeLogicalConverter implements LogicalTypeConverter {

  @Override
  public JsonNode toJson(final Schema schema, final Object value, final JsonConverterConfig config) {
    if (!(value instanceof java.util.Date))
      throw new DataException("Invalid type for Date, expected Date but was " + value.getClass());
    return JsonNodeFactory.instance.numberNode(Date.fromLogical(schema, (java.util.Date) value));
  }

  @Override
  public Object toConnect(final Schema schema, final JsonNode value) {
    String valueAsText = value.asText();
    String pattern = "YYYYMMDDHHmmss";
    SimpleDateFormat dateFormat = new SimpleDateFormat(pattern);
    java.util.Date parsedDate = null;
    try {
      parsedDate = dateFormat.parse(valueAsText);
      return parsedDate;
    } catch (Exception ex){
      throw new DataException("Invalid type for Date, underlying representation should be integral but was " + value.getNodeType()+ " "+valueAsText+" "+parsedDate);
    }

  }
}
