package com.purbon.kafka.connect.converters;

import java.util.List;

public class JsonConversionUtils {


  private final JsonConverterConfig config;

  public JsonConversionUtils(JsonConverterConfig config) {
    this.config = config;
  }

  public  boolean isStringAnInteger(String value) {
    try {
      Integer.valueOf(value);
      return true;
    } catch (Exception ex){
      return false;
    }
  }

  public  boolean isStringAFloat(String value) {
    try {
      Float.valueOf(value);
      return true;
    } catch (Exception ex){
      return false;
    }
  }

  public boolean isStringADate(String key, String value) {
    List<String> dateAttributes = config.getDateAttributes();
    if (key.toLowerCase().endsWith("_dt") || dateAttributes.contains(key)) {
      return true;
    } else {
      return false;
    }
  }

  public boolean isStringATimestamp(String key, String value) {
    List<String> timestampAttributes = config.getTimestampAttributes();
    if (key.toLowerCase().endsWith("_ts") || timestampAttributes.contains(key)) {
      return true;
    } else {
      return false;
    }
  }
}
