package com.purbon.kafka.connect.converters;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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

  public static final String TS_ATTRS_CONFIG = "timestamp.attrs";
  public static final List<String> TS_ATTRS_DEFAULT = new ArrayList<>();
  private static final String TS_ATTRS_DOC = "List of attributes to be converted to Timestamp";
  private static final String TS_ATTRS_DISPLAY = "Attributes with TS data type";

  public static final String DT_ATTRS_CONFIG = "date.attrs";
  public static final List<String> DT_ATTRS_DEFAULT = new ArrayList<>();
  private static final String DT_ATTRS_DOC = "List of attributes to be converted to Date";
  private static final String DT_ATTRS_DISPLAY = "Attributes with DT data type";


  private final static ConfigDef CONFIG;

  static {
    String group = "JsonConversion";
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
        SCHEMAS_ENABLE_DISPLAY)
        .define( TS_ATTRS_CONFIG,
        Type.LIST,
        TS_ATTRS_DEFAULT,
        Importance.HIGH,
        TS_ATTRS_DOC,
        group,
        orderInGroup++,
        Width.MEDIUM,
        TS_ATTRS_DISPLAY
        ).define( DT_ATTRS_CONFIG,
        Type.LIST,
        DT_ATTRS_DEFAULT,
        Importance.HIGH,
        DT_ATTRS_DOC,
        group,
        orderInGroup++,
        Width.MEDIUM,
        DT_ATTRS_DISPLAY
    );
  }

  public static ConfigDef configDef() {
    return CONFIG;
  }

  private boolean schemasEnabled;
  private List<String> timestampAttributes;
  private List<String> dateAttributes;

  protected JsonConverterConfig(Map<String, ?> props) {
    super(CONFIG, props);
    this.schemasEnabled = getBoolean(SCHEMAS_ENABLE_CONFIG);
    this.timestampAttributes = getList(TS_ATTRS_CONFIG);
    this.dateAttributes = getList(DT_ATTRS_CONFIG);

  }

  public List<String> getTimestampAttributes() {
    return timestampAttributes;
  }

  public boolean isSchemasEnabled() {
    return schemasEnabled;
  }

  public List<String> getDateAttributes() {
    return dateAttributes;
  }
}
