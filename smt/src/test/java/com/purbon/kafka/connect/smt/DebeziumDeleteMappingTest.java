package com.purbon.kafka.connect.smt;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.stream.Collectors;

import static com.purbon.kafka.connect.smt.DebeziumDeleteMappingOpConfig.OPERATION_DELETE_VALUE_CONF;
import static com.purbon.kafka.connect.smt.DebeziumDeleteMappingOpConfig.OPERATION_FIELD_CONF;
import static com.purbon.kafka.connect.smt.FieldsSelectionConfig.FIELDS_CONF;
import static org.assertj.core.api.Assertions.assertThat;

public class DebeziumDeleteMappingTest {

    private DebeziumDeleteMapping<SourceRecord> transform = new DebeziumDeleteMapping<SourceRecord>();

    @Test
    public void testNoDeleteOperation() {
        String topicName = "my.ingest.topic";
        SourceRecord record = new SourceRecord(null, null, topicName,0,  Schema.STRING_SCHEMA, "key", schema(), value());
        SourceRecord newRecord = transform.apply(record);
        assertThat(newRecord).isEqualTo(record);
    }

    @Test
    public void testWithDeleteOperation() {
        String topicName = "my.ingest.topic";
        SourceRecord record = new SourceRecord(null, null, topicName,0,  Schema.STRING_SCHEMA, "key", schema(), deleteValue());
        SourceRecord newRecord = transform.apply(record);
        assertThat(newRecord.value()).isNull();
    }


    @Before
    public void before() {
        var config = new HashMap<String, Object>();
        config.put(OPERATION_FIELD_CONF, "op");
        config.put(OPERATION_DELETE_VALUE_CONF, "d");
        transform.configure(config);
    }

    private Schema schema() {
        final Schema schema = SchemaBuilder
                .struct()
                .name("foo")
                .version(1)
                .doc("doc")
                .field("after", Schema.OPTIONAL_STRING_SCHEMA)
                .field("op", Schema.STRING_SCHEMA)
                .field("another_field", Schema.STRING_SCHEMA)
                .build();

        return schema;
    }


    private Struct value() {
        Struct value = new Struct(schema());
        value.put("after", "this_is_a_possible_value");
        value.put("op", "i");
        value.put("another_field", "loreminpsum");
        return value;
    }

    private Struct deleteValue() {
        Struct value = new Struct(schema());
        value.put("after", "this_is_a_possible_value");
        value.put("op", "d");
        value.put("another_field", "loreminpsum");
        return value;
    }


}
