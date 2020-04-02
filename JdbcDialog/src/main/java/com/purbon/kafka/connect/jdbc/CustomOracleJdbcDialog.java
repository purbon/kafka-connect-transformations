package com.purbon.kafka.connect.jdbc;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.dialect.DatabaseDialectProvider.SubprotocolBasedProvider;
import io.confluent.connect.jdbc.dialect.OracleDatabaseDialect;
import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;

public class CustomOracleJdbcDialog extends OracleDatabaseDialect {

  public CustomOracleJdbcDialog(AbstractConfig config) {
    super(config);
  }

  public static class Provider extends SubprotocolBasedProvider {
    public Provider() {
      super(CustomOracleJdbcDialog.class.getSimpleName(), "customOracle");
    }

    @Override
    public DatabaseDialect create(AbstractConfig config) {
      return new CustomOracleJdbcDialog(config);
    }
  }

  @Override
  protected String getSqlType(SinkRecordField field) {
    if (field.schemaName() != null) {
      switch (field.schemaName()) {
        case Decimal.LOGICAL_NAME:
          return "NUMBER(*," + field.schemaParameters().get(Decimal.SCALE_FIELD) + ")";
        case Date.LOGICAL_NAME:
          return "DATE";
        case Time.LOGICAL_NAME:
          return "DATE";
        case Timestamp.LOGICAL_NAME:
          return "TIMESTAMP";
        default:
          // fall through to normal types
      }
    }
    switch (field.schemaType()) {
      case INT8:
        return "NUMBER(3)";
      case INT16:
        return "NUMBER(5)";
      case INT32:
        return "NUMBER(10)";
      case INT64:
        return "NUMBER(20)";
      case FLOAT32:
        return "BINARY_FLOAT";
      case FLOAT64:
        return "BINARY_DOUBLE";
      case BOOLEAN:
        return "NUMBER(1,0)";
      case STRING:
        return "CLOB";
      case BYTES:
        return "BLOB";
      default:
        return super.getSqlType(field);
    }
  }


}
