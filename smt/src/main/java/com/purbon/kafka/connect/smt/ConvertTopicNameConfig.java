package com.purbon.kafka.connect.smt;

import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

public class ConvertTopicNameConfig extends AbstractConfig {

  public ConvertTopicNameConfig(ConfigDef config, Map<?, ?> originals) {
    super(config, originals);
  }

   static ConfigDef config() {
    return new ConfigDef();
  }
}

