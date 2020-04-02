package com.purbon.kafka.connect.jdbc;

import io.confluent.connect.jdbc.dialect.BaseDialectTest;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.junit.Test;

public class TestCustomOracleJdbcDialog  extends BaseDialectTest<CustomOracleJdbcDialog> {

  @Override
  protected CustomOracleJdbcDialog createDialect() {
    return new CustomOracleJdbcDialog(sourceConfigWithUrl("jdbc:customOracle://something"));
  }

  @Test
  public void testIntegerConversion() {
    assertPrimitiveMapping(Type.INT64, "NUMBER(20)");
  }

  @Test
  public void shouldMapDataTypes() {
    verifyDataTypeMapping("NUMBER(20)", Schema.INT64_SCHEMA);
    verifyDataTypeMapping("CLOB", Schema.STRING_SCHEMA);
  }
}
