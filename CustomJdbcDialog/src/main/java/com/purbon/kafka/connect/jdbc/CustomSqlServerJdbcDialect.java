package com.purbon.kafka.connect.jdbc;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.dialect.DatabaseDialectProvider;
import io.confluent.connect.jdbc.dialect.SqlServerDatabaseDialect;
import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;
import io.confluent.connect.jdbc.util.DateTimeUtils;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

public class CustomSqlServerJdbcDialect extends SqlServerDatabaseDialect {

    private static final Logger log = LoggerFactory.getLogger(CustomSqlServerJdbcDialect.class);


    public CustomSqlServerJdbcDialect(AbstractConfig config) {
        super(config);
    }

    public static class Provider extends DatabaseDialectProvider.SubprotocolBasedProvider {
        public Provider() {
            super(CustomSqlServerJdbcDialect.class.getSimpleName(), "customSqlServer");
        }

        @Override
        public DatabaseDialect create(AbstractConfig config) {
            return new CustomSqlServerJdbcDialect(config);
        }
    }

    @Override
    protected boolean maybeBindLogical(PreparedStatement statement, int index, Schema schema, Object value) throws SQLException {
        if (schema.name() != null) {
            switch (schema.name()) {
                case DebeziumTimeUnits.MILLIS_TIMESTAMP:
                    Timestamp millisTimestamp = Conversions.toTimestampFromMillis((long)value);
                    log.debug("TimeConversion[io.debezium.time.Timestamp]: value="+value+" into time="+millisTimestamp.toString());
                    String UTC_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";
                    DateFormat df = new SimpleDateFormat(UTC_DATE_FORMAT);
                    df.setTimeZone(TimeZone.getTimeZone("GMT"));
                    statement.setString(index, df.format(millisTimestamp));
                    return true;
                case DebeziumTimeUnits.NANOS_TIMESTAMP:
                    Timestamp nanoTimestamp = Conversions.toTimestampFromNanos((long)value);
                    log.debug("TimeConversion[io.debezium.time.NanoTimestamp]: value="+value+" into time="+nanoTimestamp);
                    statement.setTimestamp(index, nanoTimestamp, DateTimeUtils.getTimeZoneCalendar(timeZone())
                    );
                    return true;
                default:
                    break;
            }
        }
        return super.maybeBindLogical(statement, index, schema, value);
    }

    @Override
    protected Integer getSqlTypeForSchema(Schema schema) {
        if (schema == null) {
            return null;
        }

        switch (schema.type()) {
            case INT8:
            case INT16:
            case INT32:
                return Types.INTEGER;
            case INT64:
                return Types.BIGINT;
            case FLOAT32:
            case FLOAT64:
                return Types.FLOAT;
            case BOOLEAN:
                return Types.BOOLEAN;
            case STRING:
                return Types.VARCHAR;
            case BYTES:
                return Types.VARBINARY;
            default:
                return null;
        }
    }

    @Override
    protected String getSqlType(SinkRecordField field) {
        if (field.schemaName() != null) {
            switch (field.schemaName()) {
                case DebeziumTimeUnits.MILLIS_TIMESTAMP:
                    return "datetime";
                case DebeziumTimeUnits.NANOS_TIMESTAMP:
                    return "datetime2(7)";
                default:
                    // pass
            }
        }
        return super.getSqlType(field);
    }
}
