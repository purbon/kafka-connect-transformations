package com.purbon.kafka.connect.jdbc;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.dialect.DatabaseDialectProvider;
import io.confluent.connect.jdbc.dialect.GenericDatabaseDialect;
import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;
import io.confluent.connect.jdbc.util.IdentifierRules;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;

public class H2Dialect extends GenericDatabaseDialect {

    public static class Provider extends DatabaseDialectProvider.SubprotocolBasedProvider {
        public Provider() {
            super(H2Dialect.class.getSimpleName(), "h2");
        }

        @Override
        public DatabaseDialect create(AbstractConfig config) {
            return new H2Dialect(config);
        }
    }

    public H2Dialect(AbstractConfig config) {
        super(config, new IdentifierRules(".", "\"", "\""));
    }

    @Override
    protected String getSqlType(SinkRecordField field) {
        if (field.schemaName() != null) {
            switch (field.schemaName()) {
                case Decimal.LOGICAL_NAME:
                    return "DECIMAL(31," + field.schemaParameters().get(Decimal.SCALE_FIELD) + ")";
                case Date.LOGICAL_NAME:
                    return "DATE";
                case Time.LOGICAL_NAME:
                    return "TIME";
                case Timestamp.LOGICAL_NAME:
                    return "TIMESTAMP";
                default:
                    // fall through to normal types
            }
        }
        switch (field.schemaType()) {
            case INT8:
                return "SMALLINT";
            case INT16:
                return "SMALLINT";
            case INT32:
                return "INTEGER";
            case INT64:
                return "BIGINT";
            case FLOAT32:
                return "REAL";
            case FLOAT64:
                return "DOUBLE PRECISION";
            case BOOLEAN:
                return "BOOLEAN";
            case STRING:
                return "VARCHAR(32672)";
            case BYTES:
                return "BLOB(64000)";
            default:
                return super.getSqlType(field);
        }
    }
}
