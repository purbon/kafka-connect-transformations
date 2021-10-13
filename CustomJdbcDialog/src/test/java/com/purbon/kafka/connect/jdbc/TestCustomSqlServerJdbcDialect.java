package com.purbon.kafka.connect.jdbc;


import io.confluent.connect.jdbc.dialect.BaseDialectTest;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Timestamp;
import org.junit.Test;

import java.sql.SQLException;

public class TestCustomSqlServerJdbcDialect  extends BaseDialectTest<CustomSqlServerJdbcDialect> {

    @Override
    protected CustomSqlServerJdbcDialect createDialect() {
        return new CustomSqlServerJdbcDialect(sourceConfigWithUrl("jdbc:customSqlServer://something"));
    }

    @Test
    public void shouldMapDataTypes() {
        verifyDataTypeMapping("bigint", Schema.INT64_SCHEMA);
        verifyDataTypeMapping("varchar(max)", Schema.STRING_SCHEMA);

        var debeziumNanoTimeSchema = SchemaBuilder.int64().name(DebeziumTimeUnits.NANOS_TIMESTAMP).version(1).build();
        verifyDataTypeMapping("datetime2(7)", debeziumNanoTimeSchema);

        var debeziumMillisTimeSchema = SchemaBuilder.int64().name(DebeziumTimeUnits.MILLIS_TIMESTAMP).version(1).build();
        verifyDataTypeMapping("datetime", debeziumMillisTimeSchema);
    }

    @Override
    public void bindFieldNull() throws SQLException {

    }
}