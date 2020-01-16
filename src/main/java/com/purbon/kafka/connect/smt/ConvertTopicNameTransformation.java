package com.purbon.kafka.connect.smt;

import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;

/**
 * Convert topic name from CamelCase format to CAMEL_CASE format
 * @param <R>
 */
public class ConvertTopicNameTransformation<R extends ConnectRecord<R>> implements Transformation<R> {

  private ConvertTopicNameConfig config;

  String regexp = "(?<!(^|[A-Z]))(?=[A-Z])|(?<!^)(?=[A-Z][a-z])";

  public R apply(R record) {
    return record.newRecord(normaliseTopicName(record.topic()), record.kafkaPartition(), record.keySchema(),
        record.key(), record.valueSchema(), record.value(), record.timestamp());
  }

  private String normaliseTopicName(String topic) {
    String[] bits = topic.split(regexp);
    StringBuilder sb = new StringBuilder();
    for(int i=0; i < bits.length; i++) {
      if (i>0) {
        sb.append("_");
      }
      sb.append(bits[i].toUpperCase());
    }
    return sb.toString();
  }

  public ConfigDef config() {
    return ConvertTopicNameConfig.config();
  }

  public void close() {

  }

  public void configure(Map<String, ?> map) {
    this.config = new ConvertTopicNameConfig(config(), map);
  }
}
