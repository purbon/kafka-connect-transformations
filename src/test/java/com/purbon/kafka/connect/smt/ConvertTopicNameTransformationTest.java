package com.purbon.kafka.connect.smt;

import static org.junit.Assert.assertEquals;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

public class ConvertTopicNameTransformationTest {

  private ConvertTopicNameTransformation<SourceRecord> transform = new ConvertTopicNameTransformation<SourceRecord>();

  @Test
  public void testTopicNameChange() {

    String topicName = "CamelCase";
    SourceRecord record = new SourceRecord(null, null, topicName, 0, schema(), value());

    SourceRecord newRecord = transform.apply(record);

    assertEquals("CAMEL_CASE", newRecord.topic());
  }

  private Schema schema() {
    final Schema schema = SchemaBuilder
        .struct()
        .name("foo")
        .version(1)
        .doc("doc")
        .field("foo", Schema.STRING_SCHEMA)
        .build();

    return schema;
  }

  private Struct value() {
    Struct value = new Struct(schema());
    value.put("foo", "bar");
    return value;
  }
}
