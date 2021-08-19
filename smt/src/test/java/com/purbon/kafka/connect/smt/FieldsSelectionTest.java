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

import static com.purbon.kafka.connect.smt.FieldsSelectionConfig.FIELDS_CONF;
import static org.assertj.core.api.Assertions.assertThat;

public class FieldsSelectionTest {

    private FieldsSelection<SourceRecord> transform = new FieldsSelection<SourceRecord>();

    @Test
    public void testFieldSelectionWithAllValues() {

        String topicName = "my.ingest.topic";

        SourceRecord record = new SourceRecord(null, null, topicName,0,  Schema.STRING_SCHEMA, "key", schema(), value());
        SourceRecord newRecord = transform.apply(record);

        assertThat(((Struct)newRecord.value()).getWithoutDefault("after")).isNotNull();
        var allFieldNames = newRecord.valueSchema().fields().stream().map(Field::name).collect(Collectors.toList());
        assertThat(allFieldNames).doesNotContain("another_field");
        assertThat(newRecord.key()).isEqualTo("key");
    }

    @Test
    public void testFieldSelectionWithNullValues() {
        String topicName = "my.ingest.topic";
        SourceRecord record = new SourceRecord(null, null, topicName, 0, Schema.STRING_SCHEMA, "key", schema(), valueWithNullAfter());

        SourceRecord newRecord = transform.apply(record);

        assertThat(((Struct)newRecord.value()).getWithoutDefault("after")).isNotNull();
        assertThat(((Struct)newRecord.value()).getWithoutDefault("after")).isEqualTo("");
        var allFieldNames = newRecord.valueSchema().fields().stream().map(Field::name).collect(Collectors.toList());
        assertThat(allFieldNames).doesNotContain("another_field");
        assertThat(newRecord.key()).isEqualTo("key");
    }

    @Test
    public void testFieldSelectionOfNonExistantFields() {
        String topicName = "my.ingest.topic";
        SourceRecord record = new SourceRecord(null, null, topicName, 0, differentSchema(), differentValue());
        SourceRecord newRecord = transform.apply(record);
        //should be empty as non fields could be selected
        assertThat(((Struct)newRecord.value()).schema().fields()).hasSize(0);
    }

    @Before
    public void before() {
        var config = new HashMap<String, Object>();
        config.put(FIELDS_CONF, "after, op");
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

    private Schema differentSchema() {
        final Schema schema = SchemaBuilder
                .struct()
                .name("foo")
                .version(1)
                .doc("doc")
                .field("a", Schema.OPTIONAL_STRING_SCHEMA)
                .field("b", Schema.STRING_SCHEMA)
                .field("c", Schema.STRING_SCHEMA)
                .build();

        return schema;
    }

    private Struct valueWithNullAfter() {
        Struct value = new Struct(schema());
        value.put("after", null);
        value.put("op", "i");
        value.put("another_field", "loreminpsum");
        return value;
    }

    private Struct value() {
        Struct value = new Struct(schema());
        value.put("after", "this_is_a_possible_value");
        value.put("op", "i");
        value.put("another_field", "loreminpsum");
        return value;
    }

    private Struct differentValue() {
        Struct value = new Struct(differentSchema());
        value.put("a", "this_is_a_possible_value");
        value.put("b", "i");
        value.put("c", "loreminpsum");
        return value;
    }
}
