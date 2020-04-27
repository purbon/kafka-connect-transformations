package com.purbon.kafka.connect.converters;

import com.fasterxml.jackson.databind.JsonNode;
import com.purbon.kafka.connect.converters.json.ArrayConnectTypeConverter;
import com.purbon.kafka.connect.converters.json.DateTypeLogicalConverter;
import com.purbon.kafka.connect.converters.json.DecimalLogicalTypeConverter;
import com.purbon.kafka.connect.converters.json.JsonToConnectTypeConverter;
import com.purbon.kafka.connect.converters.json.LogicalTypeConverter;
import com.purbon.kafka.connect.converters.json.MapConnectTypeConverter;
import com.purbon.kafka.connect.converters.json.StructConnectTypeConverter;
import com.purbon.kafka.connect.converters.json.TimeLogicalTypeConverter;
import com.purbon.kafka.connect.converters.json.TimestampLogicalTypeConverter;
import java.io.IOException;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;

public class DataTypeConverter {

  private final HashMap<String, LogicalTypeConverter> LOGICAL_CONVERTERS = new HashMap<>();
  private final Map<Type, JsonToConnectTypeConverter> TO_CONNECT_CONVERTERS = new EnumMap<>(Schema.Type.class);
  private final JsonConverterConfig config;


  public DataTypeConverter(JsonConverterConfig config) {
    this.config = config;
    configureConnectConverters(config);
    configureLogicalConverters(config);
  }

  public Object convertToConnect(Schema schema, JsonNode jsonValue) {
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

  private void configureLogicalConverters(JsonConverterConfig config) {
    LOGICAL_CONVERTERS.put(Decimal.LOGICAL_NAME, new DecimalLogicalTypeConverter());
    LOGICAL_CONVERTERS.put(Date.LOGICAL_NAME, new DateTypeLogicalConverter(config));
    LOGICAL_CONVERTERS.put(Time.LOGICAL_NAME, new TimeLogicalTypeConverter());
    LOGICAL_CONVERTERS.put(Timestamp.LOGICAL_NAME, new TimestampLogicalTypeConverter(config));
  }

  private void configureConnectConverters(JsonConverterConfig config) {
    TO_CONNECT_CONVERTERS.put(Schema.Type.BOOLEAN, (schema, value) -> Boolean.valueOf(value.asText()));
    TO_CONNECT_CONVERTERS.put(Schema.Type.INT8, (schema, value) -> Byte.valueOf(value.asText()));
    TO_CONNECT_CONVERTERS.put(Schema.Type.INT16, (schema, value) -> Short.valueOf(value.asText()));
    TO_CONNECT_CONVERTERS.put(Schema.Type.INT32, (schema, value) -> Integer.valueOf(value.asText()));
    TO_CONNECT_CONVERTERS.put(Schema.Type.INT64, (schema, value) -> Long.valueOf(value.asText()));
    TO_CONNECT_CONVERTERS.put(Schema.Type.FLOAT32, (schema, value) -> Float.valueOf(value.asText()));
    TO_CONNECT_CONVERTERS.put(Schema.Type.FLOAT64, (schema, value) -> Double.valueOf(value.asText()));
    TO_CONNECT_CONVERTERS.put(Schema.Type.BYTES, (schema, value) -> {
      try {
        return value.binaryValue();
      } catch (IOException e) {
        throw new DataException("Invalid bytes field", e);
      }
    });
    TO_CONNECT_CONVERTERS.put(Schema.Type.STRING, (schema, value) -> value.textValue());
    TO_CONNECT_CONVERTERS.put(Schema.Type.ARRAY, new ArrayConnectTypeConverter(this));

    TO_CONNECT_CONVERTERS.put(Schema.Type.MAP, new MapConnectTypeConverter(this));
    TO_CONNECT_CONVERTERS.put(Schema.Type.STRUCT, new StructConnectTypeConverter(this));
  }

}
