package com.purbon.kafka.connect.smt;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DebeziumDeleteMappingOpConfig extends AbstractConnectConfig {

  public static final String OPERATION_FIELD_CONF = "op.field";
  static final String OPERATION_FIELD_DOC = "Field where the operation is record";
  static final String OPERATION_FIELDS_DEFAULT = "op";

  public static final String OPERATION_DELETE_VALUE_CONF = "op.delete.value";
  static final String OPERATION_DELETE_VALUE_DOC = "Field where the operation is record";
  static final String OPERATION_DELETE_VALUE_DEFAULT = "d";

  private String operation;
  private String operationValueForDelete;

  public DebeziumDeleteMappingOpConfig(ConfigDef config, Map<?, ?> originals) {
    super(config, originals);

    this.operation = getString(OPERATION_FIELD_CONF);
    this.operationValueForDelete = getString(OPERATION_DELETE_VALUE_CONF);
  }

  public DebeziumDeleteMappingOpConfig(Map<?, ?> originals) {
    this(config(), originals);

  }

  static ConfigDef config() {
    return AbstractConnectConfig.config()
            .define(OPERATION_FIELD_CONF,
                    Type.STRING,
                    OPERATION_FIELDS_DEFAULT,
                    Importance.MEDIUM,
                    OPERATION_FIELD_DOC)
            .define(OPERATION_DELETE_VALUE_CONF,
                    Type.STRING,
                    OPERATION_DELETE_VALUE_DEFAULT,
                    Importance.MEDIUM,
                    OPERATION_DELETE_VALUE_DOC);
  }

  public String getOperation() {
    return operation;
  }

  public String getOperationValueForDelete() {
    return operationValueForDelete;
  }
}
