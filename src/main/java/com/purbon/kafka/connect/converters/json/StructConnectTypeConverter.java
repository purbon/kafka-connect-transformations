package com.purbon.kafka.connect.converters.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.purbon.kafka.connect.converters.DataTypeConverter;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;

public class StructConnectTypeConverter implements JsonToConnectTypeConverter {

  private final DataTypeConverter converter;

  public StructConnectTypeConverter(DataTypeConverter converter) {
    this.converter = converter;
  }

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
      result.put(field, converter.convertToConnect(field.schema(), value.get(field.name())));

    return result;
  }
}
