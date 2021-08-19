package com.purbon.kafka.connect.smt;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class FieldsSelection<R extends ConnectRecord<R>> implements Transformation<R> {

    private FieldsSelectionConfig config;

    /**
     * Actions:
     *   one transform
     *   - Select list of fields the after and op.
     *   - If null, transform into default value. (should not break!!!)
     *  another tansform:
     *   - if op == 'd' then create thumbstons (key=key, value=null) else (forward the record).
     */

    @Override
    public R apply(R record) {

        List<String> fieldsToBeSelected = config.getFieldsToBeSelected();

        Struct recordValue = (Struct) record.value();
        Schema recordSchema = record.valueSchema();

        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(recordSchema, SchemaBuilder.struct());

        for(Field field : recordSchema.fields()) {
            if (fieldsToBeSelected.contains(field.name())) {
                builder.field(field.name(), field.schema());
            }
        }

        Schema newRecordSchema = builder.schema();
        Struct newRecordValue = new Struct(newRecordSchema);

        for(Field field : newRecordSchema.fields()) {
            Object objectValue = recordValue.getWithoutDefault(field.name());
            if (objectValue == null) {
                Schema fieldSchema = newRecordSchema.field(field.name()).schema();
                objectValue = defaultValueForSchema(fieldSchema);
            }
            newRecordValue.put(field, objectValue);
        }

        return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(),
                newRecordSchema, newRecordValue, record.timestamp());
    }

    private Object defaultValueForSchema(Schema fieldSchema) {
        var type = fieldSchema.type();
        switch (fieldSchema.type()) {
            case STRING:
                return "";
            case INT8:
            case INT16:
            case INT32:
            case INT64:
                return 0;
            case FLOAT32:
            case FLOAT64:
                return 0.0f;
            case ARRAY:
                return Collections.emptyList();
            case MAP:
                return Collections.emptyMap();
            case BOOLEAN:
                return false;
            default:
                return "null";
        }
    }


    @Override
    public ConfigDef config() {
        return FieldsSelectionConfig.config();
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> config) {
        this.config = new FieldsSelectionConfig(config(), config);
    }
}
