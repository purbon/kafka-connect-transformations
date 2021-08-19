package com.purbon.kafka.connect.smt;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.NonEmptyString;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class FieldsSelectionConfig extends AbstractConnectConfig {

  public static final String FIELDS_CONF = "fields";
  static final String FIELDS_DOC = "Fields to be selected form the incoming JSON payload";
  static final String FIELDS_DEFAULT = "";

  private List<String> fieldsToBeSelected;

  public FieldsSelectionConfig(ConfigDef config, Map<?, ?> originals) {
    super(config, originals);

    var arrayOfStrings = getString(FIELDS_CONF).split(",");
    this.fieldsToBeSelected = Arrays.stream(arrayOfStrings).map(String::trim).collect(Collectors.toList());
  }

  public FieldsSelectionConfig(Map<?, ?> originals) {
    this(config(), originals);

  }

  static ConfigDef config() {
    return AbstractConnectConfig.config()
            .define(FIELDS_CONF,
                    Type.STRING,
                    FIELDS_DEFAULT,
                    Importance.MEDIUM,
                    FIELDS_DOC);
  }

  public List<String> getFieldsToBeSelected() {
    return fieldsToBeSelected;
  }

}
