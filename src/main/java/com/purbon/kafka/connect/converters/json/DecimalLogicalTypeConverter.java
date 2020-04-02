package com.purbon.kafka.connect.converters.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.purbon.kafka.connect.converters.JsonConverterConfig;
import java.math.BigDecimal;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;

public class DecimalLogicalTypeConverter implements LogicalTypeConverter {

  @Override
  public JsonNode toJson(final Schema schema, final Object value, final JsonConverterConfig config) {
    if (!(value instanceof BigDecimal))
      throw new DataException("Invalid type for Decimal, expected BigDecimal but was " + value.getClass());

    final BigDecimal decimal = (BigDecimal) value;
    return JsonNodeFactory.instance.numberNode(decimal);
  }

  @Override
  public Object toConnect(final Schema schema, final JsonNode value) {
    if (value.isNumber()) return value.decimalValue();
    if (value.isBinary() || value.isTextual()) {
      try {
        return Decimal.toLogical(schema, value.binaryValue());
      } catch (Exception e) {
        throw new DataException("Invalid bytes for Decimal field", e);
      }
    }

    throw new DataException("Invalid type for Decimal, underlying representation should be numeric or bytes but was " + value.getNodeType());
  }
}
