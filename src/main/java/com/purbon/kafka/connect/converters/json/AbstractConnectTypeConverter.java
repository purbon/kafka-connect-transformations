package com.purbon.kafka.connect.converters.json;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;

public abstract class AbstractConnectTypeConverter implements JsonToConnectTypeConverter {

  // Convert values in Kafka Connect form into/from their logical types. These logical converters are discovered by logical type
  // names specified in the field
  private static final HashMap<String, LogicalTypeConverter> LOGICAL_CONVERTERS = new HashMap<>();
  static {
    LOGICAL_CONVERTERS.put(Decimal.LOGICAL_NAME, new DecimalLogicalTypeConverter());
    LOGICAL_CONVERTERS.put(Date.LOGICAL_NAME, new DateTypeLogicalConverter());
    LOGICAL_CONVERTERS.put(Time.LOGICAL_NAME, new TimeLogicalTypeConverter());
    LOGICAL_CONVERTERS.put(Timestamp.LOGICAL_NAME, new TimestampLogicalTypeConverter());
  }

  private static final Map<Schema.Type, JsonToConnectTypeConverter> TO_CONNECT_CONVERTERS = new EnumMap<>(Schema.Type.class);

  static {
    TO_CONNECT_CONVERTERS.put(Schema.Type.BOOLEAN, (schema, value) -> value.booleanValue());
    TO_CONNECT_CONVERTERS.put(Schema.Type.INT8, (schema, value) -> (byte) value.intValue());
    TO_CONNECT_CONVERTERS.put(Schema.Type.INT16, (schema, value) -> (short) value.intValue());
    TO_CONNECT_CONVERTERS.put(Schema.Type.INT32, (schema, value) -> value.intValue());
    TO_CONNECT_CONVERTERS.put(Schema.Type.INT64, (schema, value) -> value.longValue());
    TO_CONNECT_CONVERTERS.put(Schema.Type.FLOAT32, (schema, value) -> value.floatValue());
    TO_CONNECT_CONVERTERS.put(Schema.Type.FLOAT64, (schema, value) -> value.doubleValue());
    TO_CONNECT_CONVERTERS.put(Schema.Type.BYTES, (schema, value) -> {
      try {
        return value.binaryValue();
      } catch (IOException e) {
        throw new DataException("Invalid bytes field", e);
      }
    });
    TO_CONNECT_CONVERTERS.put(Schema.Type.STRING, (schema, value) -> value.textValue());
    TO_CONNECT_CONVERTERS.put(Schema.Type.ARRAY, new ArrayConnectTypeConverter());

    TO_CONNECT_CONVERTERS.put(Schema.Type.MAP, new MapConnectTypeConverter());
    TO_CONNECT_CONVERTERS.put(Schema.Type.STRUCT, new StructConnectTypeConverter());
  }

  public static Object convertToConnect(Schema schema, JsonNode jsonValue) {
    final Schema.Type schemaType;
    if (schema != null) {
      schemaType = schema.type();
      if (jsonValue == null || jsonValue.isNull()) {
        if (schema.defaultValue() != null)
          return schema
              .defaultValue(); // any logical type conversions should already have been applied
        if (schema.isOptional())
          return null;
        throw new DataException("Invalid null value for required " + schemaType + " field");
      }
    } else {
      switch (jsonValue.getNodeType()) {
        case NULL:
          // Special case. With no schema
          return null;
        case BOOLEAN:
          schemaType = Schema.Type.BOOLEAN;
          break;
        case NUMBER:
          if (jsonValue.isIntegralNumber())
            schemaType = Schema.Type.INT64;
          else
            schemaType = Schema.Type.FLOAT64;
          break;
        case ARRAY:
          schemaType = Schema.Type.ARRAY;
          break;
        case OBJECT:
          schemaType = Schema.Type.MAP;
          break;
        case STRING:
          schemaType = Schema.Type.STRING;
          break;

        case BINARY:
        case MISSING:
        case POJO:
        default:
          schemaType = null;
          break;
      }
    }

    final JsonToConnectTypeConverter typeConverter = TO_CONNECT_CONVERTERS.get(schemaType);
    if (typeConverter == null)
      throw new DataException("Unknown schema type: " + schemaType);

    if (schema != null && schema.name() != null) {
      LogicalTypeConverter logicalConverter = LOGICAL_CONVERTERS.get(schema.name());
      if (logicalConverter != null)
        return logicalConverter.toConnect(schema, jsonValue);
    }

    return typeConverter.convert(schema, jsonValue);
  }

}
