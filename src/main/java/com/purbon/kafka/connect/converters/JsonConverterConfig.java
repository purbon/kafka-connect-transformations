package com.purbon.kafka.connect.converters;

import java.util.ArrayList;
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

  public static final  String TS_PATTERN_CONFIG = "timestamp.pattern";
  public static final  String TS_PATTERN_DEFAULT = "YYYYMMDDHHmmssSSSSSSS";
  private static final String TS_PATTERN_DOC = "Pattern used to detect the incoming timestamp value";
  private static final String TS_PATTERN_DISPLAY = "Timestamp pattern value";

  public static final String DT_ATTRS_CONFIG = "date.attrs";
  public static final List<String> DT_ATTRS_DEFAULT = new ArrayList<>();
  private static final String DT_ATTRS_DOC = "List of attributes to be converted to Date";
  private static final String DT_ATTRS_DISPLAY = "Attributes with DT data type";

  public static final  String DT_PATTERN_CONFIG = "date.pattern";
  public static final  String DT_PATTERN_DEFAULT = "YYYYMMDDHHmmss";
  private static final String DT_PATTERN_DOC = "Pattern used to detect the incoming date value";
  private static final String DT_PATTERN_DISPLAY = "Date pattern value";


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
        ).define( TS_PATTERN_CONFIG,
        Type.STRING,
        TS_PATTERN_DEFAULT,
        Importance.MEDIUM,
        TS_PATTERN_DOC,
        group,
        orderInGroup++,
        Width.MEDIUM,
        TS_PATTERN_DISPLAY
    ).define( DT_ATTRS_CONFIG,
        Type.LIST,
        DT_ATTRS_DEFAULT,
        Importance.HIGH,
        DT_ATTRS_DOC,
        group,
        orderInGroup++,
        Width.MEDIUM,
        DT_ATTRS_DISPLAY
    ).define( DT_PATTERN_CONFIG,
        Type.STRING,
        DT_PATTERN_DEFAULT,
        Importance.MEDIUM,
        DT_PATTERN_DOC,
        group,
        orderInGroup++,
        Width.MEDIUM,
        DT_PATTERN_DISPLAY
    );
  }

  public static ConfigDef configDef() {
    return CONFIG;
  }

  private boolean schemasEnabled;
  private List<String> timestampAttributes;
  private String timestampPattern;
  private List<String> dateAttributes;
  private String datePattern;

  protected JsonConverterConfig(Map<String, ?> props) {
    super(CONFIG, props);
    this.schemasEnabled = getBoolean(SCHEMAS_ENABLE_CONFIG);
    this.timestampAttributes = getList(TS_ATTRS_CONFIG);
    this.timestampPattern = getString(TS_PATTERN_CONFIG);
    this.dateAttributes = getList(DT_ATTRS_CONFIG);
    this.datePattern = getString(DT_PATTERN_CONFIG);
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

  public String getTimestampPattern() {
    return timestampPattern;
  }

  public String getDatePattern() {
    return datePattern;
  }
}
