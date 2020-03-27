package com.purbon.kafka.connect.converters;

import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.connect.storage.ConverterConfig;

public class JsonConverterConfig extends ConverterConfig {

  public static final String SCHEMAS_ENABLE_CONFIG = "schemas.enable";
  public static final boolean SCHEMAS_ENABLE_DEFAULT = true;
  private static final String SCHEMAS_ENABLE_DOC = "Include schemas within each of the serialized values and keys.";
  private static final String SCHEMAS_ENABLE_DISPLAY = "Enable Schemas";

  private final static ConfigDef CONFIG;

  static {
    String group = "Schemas";
    int orderInGroup = 0;
    CONFIG = ConverterConfig.newConfigDef();
    CONFIG.define(SCHEMAS_ENABLE_CONFIG,
        Type.BOOLEAN,
        SCHEMAS_ENABLE_DEFAULT,
        Importance.HIGH,
        SCHEMAS_ENABLE_DOC,
        group,
        orderInGroup++,
        Width.MEDIUM,
        SCHEMAS_ENABLE_DISPLAY);
  }

  public static ConfigDef configDef() {
    return CONFIG;
  }


  protected JsonConverterConfig(Map<String, ?> props) {
    super(CONFIG, props);
  }
}
