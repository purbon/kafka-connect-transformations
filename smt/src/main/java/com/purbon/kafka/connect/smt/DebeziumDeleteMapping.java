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

public class DebeziumDeleteMapping<R extends ConnectRecord<R>> implements Transformation<R> {

    private DebeziumDeleteMappingOpConfig config;

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

        String operationField = config.getOperation();
        String operationValueForDelete = config.getOperationValueForDelete();

        Struct recordValue = (Struct) record.value();
        Schema recordSchema = record.valueSchema();

        if (recordSchema.field(operationField) != null) {
            String operationValue = recordValue.getString(operationField);
            if (operationValue.equalsIgnoreCase(operationValueForDelete)) {
                return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(),
                        record.valueSchema(), null, record.timestamp());
            }
        }
        return record;
    }


    @Override
    public ConfigDef config() {
        return DebeziumDeleteMappingOpConfig.config();
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> config) {
        this.config = new DebeziumDeleteMappingOpConfig(config(), config);
    }
}
