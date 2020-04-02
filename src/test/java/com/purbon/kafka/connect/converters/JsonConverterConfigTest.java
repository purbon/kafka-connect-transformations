package com.purbon.kafka.connect.converters;

import static org.junit.Assert.assertEquals;

import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.connect.storage.ConverterType;
import org.junit.Test;

public class JsonConverterConfigTest {

  @Test
  public void shouldProcessTSTypesProperly() {
    Map<String, Object> configValues = new HashMap<>();
    configValues.put(ConverterConfig.TYPE_CONFIG, ConverterType.KEY.getName());
    configValues.put(JsonConverterConfig.TS_ATTRS_CONFIG, Arrays.asList("NuMeRiC"));

    final JsonConverterConfig config = new JsonConverterConfig(configValues);
    assertEquals(config.getTimestampAttributes().size(),1 );
  }

  @Test
  public void shouldProcessDTTypesProperly() {
    Map<String, Object> configValues = new HashMap<>();
    configValues.put(ConverterConfig.TYPE_CONFIG, ConverterType.KEY.getName());
    configValues.put(JsonConverterConfig.DT_ATTRS_CONFIG, Arrays.asList("NuMeRiC"));

    final JsonConverterConfig config = new JsonConverterConfig(configValues);
    assertEquals(config.getDateAttributes().size(),1 );
  }

}
