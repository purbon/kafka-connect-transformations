package com.purbon.kafka.connect.jdbc.dialect;

import io.confluent.connect.jdbc.dialect.OracleDatabaseDialect;
import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;

public class CustomOracleDatabaseDialog extends OracleDatabaseDialect {

  public CustomOracleDatabaseDialog(AbstractConfig config) {
    super(config);
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
      case INT16:
      case INT32:
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
