package com.purbon.kafka.connect.converters;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Base64;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;

public class JSONConverter implements Converter, HeaderConverter {

  private static final Map<Schema.Type, JsonToConnectTypeConverter> TO_CONNECT_CONVERTERS = new EnumMap<>(Schema.Type.class);

  static {
    TO_CONNECT_CONVERTERS.put(Schema.Type.BOOLEAN, new JsonToConnectTypeConverter() {
      @Override
      public Object convert(Schema schema, JsonNode value) {
        return value.booleanValue();
      }
    });
    TO_CONNECT_CONVERTERS.put(Schema.Type.INT8, new JsonToConnectTypeConverter() {
      @Override
      public Object convert(Schema schema, JsonNode value) {
        return (byte) value.intValue();
      }
    });
    TO_CONNECT_CONVERTERS.put(Schema.Type.INT16, new JsonToConnectTypeConverter() {
      @Override
      public Object convert(Schema schema, JsonNode value) {
        return (short) value.intValue();
      }
    });
    TO_CONNECT_CONVERTERS.put(Schema.Type.INT32, new JsonToConnectTypeConverter() {
      @Override
      public Object convert(Schema schema, JsonNode value) {
        return value.intValue();
      }
    });
    TO_CONNECT_CONVERTERS.put(Schema.Type.INT64, new JsonToConnectTypeConverter() {
      @Override
      public Object convert(Schema schema, JsonNode value) {
        return value.longValue();
      }
    });
    TO_CONNECT_CONVERTERS.put(Schema.Type.FLOAT32, new JsonToConnectTypeConverter() {
      @Override
      public Object convert(Schema schema, JsonNode value) {
        return value.floatValue();
      }
    });
    TO_CONNECT_CONVERTERS.put(Schema.Type.FLOAT64, new JsonToConnectTypeConverter() {
      @Override
      public Object convert(Schema schema, JsonNode value) {
        return value.doubleValue();
      }
    });
    TO_CONNECT_CONVERTERS.put(Schema.Type.BYTES, new JsonToConnectTypeConverter() {
      @Override
      public Object convert(Schema schema, JsonNode value) {
        try {
          return value.binaryValue();
        } catch (IOException e) {
          throw new DataException("Invalid bytes field", e);
        }
      }
    });
    TO_CONNECT_CONVERTERS.put(Schema.Type.STRING, new JsonToConnectTypeConverter() {
      @Override
      public Object convert(Schema schema, JsonNode value) {
        return value.textValue();
      }
    });
    TO_CONNECT_CONVERTERS.put(Schema.Type.ARRAY, new JsonToConnectTypeConverter() {
      @Override
      public Object convert(Schema schema, JsonNode value) {
        Schema elemSchema = schema == null ? null : schema.valueSchema();
        ArrayList<Object> result = new ArrayList<>();
        for (JsonNode elem : value) {
          result.add(convertToConnect(elemSchema, elem));
        }
        return result;
      }
    });
    TO_CONNECT_CONVERTERS.put(Schema.Type.MAP, new JsonToConnectTypeConverter() {
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
          Iterator<Map.Entry<String, JsonNode>> fieldIt = value.fields();
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
    });
    TO_CONNECT_CONVERTERS.put(Schema.Type.STRUCT, new JsonToConnectTypeConverter() {
      @Override
      public Object convert(Schema schema, JsonNode value) {
        if (!value.isObject())
          throw new DataException("Structs should be encoded as JSON objects, but found " + value.getNodeType());

        // We only have ISchema here but need Schema, so we need to materialize the actual schema. Using ISchema
        // avoids having to materialize the schema for non-Struct types but it cannot be avoided for Structs since
        // they require a schema to be provided at construction. However, the schema is only a SchemaBuilder during
        // translation of schemas to JSON; during the more common translation of data to JSON, the call to schema.schema()
        // just returns the schema Object and has no overhead.
        Struct result = new Struct(schema.schema());
        for (Field field : schema.fields())
          result.put(field, convertToConnect(field.schema(), value.get(field.name())));

        return result;
      }
    });
  }

  // Convert values in Kafka Connect form into/from their logical types. These logical converters are discovered by logical type
  // names specified in the field
  private static final HashMap<String, LogicalTypeConverter> LOGICAL_CONVERTERS = new HashMap<>();
  static {
    LOGICAL_CONVERTERS.put(Decimal.LOGICAL_NAME, new LogicalTypeConverter() {
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
    });

    LOGICAL_CONVERTERS.put(Date.LOGICAL_NAME, new LogicalTypeConverter() {
      @Override
      public JsonNode toJson(final Schema schema, final Object value, final JsonConverterConfig config) {
        if (!(value instanceof java.util.Date))
          throw new DataException("Invalid type for Date, expected Date but was " + value.getClass());
        return JsonNodeFactory.instance.numberNode(Date.fromLogical(schema, (java.util.Date) value));
      }

      @Override
      public Object toConnect(final Schema schema, final JsonNode value) {
        if (!(value.isInt()))
          throw new DataException("Invalid type for Date, underlying representation should be integer but was " + value.getNodeType());
        return Date.toLogical(schema, value.intValue());
      }
    });

    LOGICAL_CONVERTERS.put(Time.LOGICAL_NAME, new LogicalTypeConverter() {
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
    });

    LOGICAL_CONVERTERS.put(Timestamp.LOGICAL_NAME, new LogicalTypeConverter() {
      @Override
      public JsonNode toJson(final Schema schema, final Object value, final JsonConverterConfig config) {
        if (!(value instanceof java.util.Date))
          throw new DataException("Invalid type for Timestamp, expected Date but was " + value.getClass());
        return JsonNodeFactory.instance.numberNode(Timestamp.fromLogical(schema, (java.util.Date) value));
      }

      @Override
      public Object toConnect(final Schema schema, final JsonNode value) {
        String valueAsText = value.asText();
        Long valueAsLong = -1L;
        try {
          valueAsLong = Long.valueOf(valueAsText);
          return Timestamp.toLogical(schema, valueAsLong);
        } catch (Exception ex){
          ex.printStackTrace();
          throw new DataException("Invalid type for Timestamp, underlying representation should be integral but was " + value.getNodeType()+ " "+valueAsText+" "+valueAsLong);
        }
      }
    });
  }

  ObjectMapper mapper = new ObjectMapper();

  @Override
  public void close() throws IOException {
    //empty
  }

  @Override
  public void configure(Map<String, ?> configs) {
    //TODO process config
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    configure(configs);
  }

  @Override
  public byte[] fromConnectData(String topic, Schema schema, Object value) {

    String jsonString = null;
    try {
      jsonString = mapper.writeValueAsString(value);
    } catch (JsonProcessingException e) {
      e.printStackTrace();
      throw new DataException("Json with incorrect format");
    }
    return Base64.getEncoder().encode(jsonString.getBytes());
  }

  @Override
  public SchemaAndValue toConnectData(String topic, byte[] value) {
    // This handles a tombstone message
    if (value == null) {
      return SchemaAndValue.NULL;
    }

    String jsonString = new String(value);
    Schema schema = null;
    try {
      schema = inspectJSONSchema(jsonString);
    } catch (JsonProcessingException e) {
      e.printStackTrace();
      throw new DataException("json with incorrect format");
    }

    try {
      return new SchemaAndValue(schema,
          convertToConnect(schema, mapper.readTree(jsonString)));
    } catch (JsonProcessingException e) {
      e.printStackTrace();
      throw new DataException("json with incorrect format");
    }
  }

  private Schema inspectJSONSchema(String jsonString) throws JsonProcessingException {
    TypeReference<HashMap<Object, Object>> typeRef = new TypeReference<HashMap<Object, Object>>() {
    };

    Map<Object, Object> object = mapper.readValue(jsonString, typeRef);

    SchemaBuilder struct = SchemaBuilder.struct();
    for (Object key : object.keySet()) {
      Object value = object.get(key);
      Schema schema = buildSchema(key.toString(), value);
      struct.field(key.toString(), schema);
    }

    return struct.build();
  }

  private Schema buildSchema(String key, Object value) {
    Schema schema;
    if (value instanceof Integer) {
      schema = Schema.INT64_SCHEMA;
    } else if (value instanceof Boolean) {
      schema = Schema.BOOLEAN_SCHEMA;
    } else if (value instanceof Double) {
      schema = Schema.FLOAT64_SCHEMA;
    } else if (value instanceof String) {
      String valueAsString = String.valueOf(value);
      if (isStringADate(key, valueAsString)) {
        schema = SchemaBuilder
            .string()
            .name(Date.LOGICAL_NAME)
            .build();
      } else if (isStringATimestamp(key, valueAsString)) {
        schema = SchemaBuilder
            .string()
            .name(Timestamp.LOGICAL_NAME)
            .build();
      } else if (isStringAnInteger(valueAsString)) {
        schema = Schema.INT64_SCHEMA;
      } else if (isStringAFloat(valueAsString)) {
        schema = Schema.FLOAT64_SCHEMA;
      } else {
        schema = SchemaBuilder
            .string()
            .defaultValue("")
            .optional()
            .build();
      }
    } else if (value instanceof ArrayList) {
      List list = (ArrayList) value;
      schema = SchemaBuilder
          .array(buildSchema(key, list.get(0)))
          .build();
    } else if (value instanceof HashMap) {
      SchemaBuilder builder = SchemaBuilder.struct();
      Map<String, Object> map = (Map) value;
      for (String myKey : map.keySet()) {
        builder.field(myKey, buildSchema(key, map.get(myKey)));
      }
      schema = builder.build();
    } else {
      throw new DataException(("wrong value detection of " + value));
    }
    return schema;
  }

  private boolean isStringAnInteger(String value) {
    try {
      Integer.valueOf(value);
      return true;
    } catch (Exception ex){
      return false;
    }
  }

  private boolean isStringAFloat(String value) {
    try {
      Float.valueOf(value);
      return true;
    } catch (Exception ex){
      return false;
    }
  }

  private boolean isStringADate(String key, String value) {
    if (key.toLowerCase().endsWith("_dt")) {
      return true;
    } else {
      return false;
    }
  }

  private boolean isStringATimestamp(String key, String value) {
    if (key.toLowerCase().endsWith("_ts")) {
      return true;
    } else {
      return false;
    }
  }

  @Override
  public SchemaAndValue toConnectHeader(String topic, String headerKey, byte[] value) {
    return toConnectData(topic, value);
  }

  @Override
  public byte[] fromConnectHeader(String topic, String headerKey, Schema schema, Object value) {
    return fromConnectData(topic, schema, value);
  }

  private JsonConverterConfig config;


  @Override
  public ConfigDef config() {
    return JsonConverterConfig.configDef();
  }

  private static Object convertToConnect(Schema schema, JsonNode jsonValue) {
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


  private interface JsonToConnectTypeConverter {
    Object convert(Schema schema, JsonNode value);
  }

  private interface LogicalTypeConverter {
    JsonNode toJson(Schema schema, Object value, JsonConverterConfig config);
    Object toConnect(Schema schema, JsonNode value);
  }

}
