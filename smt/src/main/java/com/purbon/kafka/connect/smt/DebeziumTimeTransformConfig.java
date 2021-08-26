package com.purbon.kafka.connect.smt;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.Map;

public class DebeziumTimeTransformConfig extends AbstractConnectConfig {

  public DebeziumTimeTransformConfig(ConfigDef config, Map<?, ?> originals) {
    super(config, originals);
  }

  public DebeziumTimeTransformConfig(Map<?, ?> originals) {
    this(config(), originals);

  }

  static ConfigDef config() {
    return AbstractConnectConfig.config();
  }
}
