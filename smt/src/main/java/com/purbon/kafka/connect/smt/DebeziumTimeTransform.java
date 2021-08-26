package com.purbon.kafka.connect.smt;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.Map;

public class DebeziumTimeTransform<R extends ConnectRecord<R>> implements Transformation<R> {

    private DebeziumTimeTransformConfig config;

    @Override
    public R apply(R record) {
        System.out.println("DEBUG: DebeziumTimeTransform");
        Schema recordSchema = record.valueSchema();
        System.out.println("DebeziumTimeTransform "+recordSchema);
        System.out.println("END");
        return record;
    }

    @Override
    public ConfigDef config() {
        return DebeziumTimeTransformConfig.config();
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> config) {
        this.config = new DebeziumTimeTransformConfig(config(), config);
    }
}
