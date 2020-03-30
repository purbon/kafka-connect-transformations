package com.purbon.kafka.connect.jdbc.dialect;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.dialect.DatabaseDialectProvider.SubprotocolBasedProvider;
import io.confluent.connect.jdbc.dialect.MySqlDatabaseDialect;
import org.apache.kafka.common.config.AbstractConfig;

public class CustomMySQLDatabaseDialect extends MySqlDatabaseDialect {

  public CustomMySQLDatabaseDialect(AbstractConfig config) {
    super(config);
  }

  /**
   * The provider for {@link MySqlDatabaseDialect}.
   */
  public static class Provider extends SubprotocolBasedProvider {
    public Provider() {
      super(MySqlDatabaseDialect.class.getSimpleName(), "mariadb", "mysql");
    }

    @Override
    public DatabaseDialect create(AbstractConfig config) {
      return new MySqlDatabaseDialect(config);
    }
  }


}
