package com.purbon.kafka.connect.converters;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Timestamp;
import java.time.temporal.TemporalField;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.connect.storage.ConverterType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import java.util.Map;

public class JsonConverterTest {


  ObjectMapper mapper = new ObjectMapper();
  JSONConverter converter = new JSONConverter();

  @Before
  public void setup() {
    Map<String, Object> objectConfig = new HashMap<>();
    objectConfig.put(ConverterConfig.TYPE_CONFIG, ConverterType.KEY.getName());
    converter.configure(objectConfig);
  }


  @Test
  public void testJsonKeyValueConversion() {

    String jsonString = "{\"bar\":\"foo\",\"foo\":2,\"zet\":[\"foo\",\"bar\"],\"props\":{\"bar\":\"foo\",\"foo\":2}}\n";

    SchemaAndValue sav =  converter.toConnectData("topic", jsonString.getBytes());

    Assert.assertEquals(Type.INT64, sav.schema().field("foo").schema().type());
    Assert.assertEquals(Type.STRING, sav.schema().field("bar").schema().type());
    Assert.assertEquals(Type.ARRAY, sav.schema().field("zet").schema().type());
    Assert.assertEquals(Type.STRUCT, sav.schema().field("props").schema().type());
    Assert.assertEquals(Type.INT64, sav.schema().field("props").schema().field("foo").schema().type());
  }

  @Test
  public void testJsonStringAsIntegersConversion() {
    String jsonString = "{\"bar\":\"12345\"}\n";
    SchemaAndValue sav = converter.toConnectData("topic", jsonString.getBytes());
    Schema schema = sav.schema();
    Assert.assertEquals(Type.INT64, schema.field("bar").schema().type());
  }

  @Test
  public void testJsonStringAsFloatConversion() {
    String jsonString = "{\"bar\":\"12345.234\"}\n";
    SchemaAndValue sav = converter.toConnectData("topic", jsonString.getBytes());
    Schema schema = sav.schema();
    Assert.assertEquals(Type.FLOAT64, schema.field("bar").schema().type());
  }

  @Test
  public void testJsonStringAsDateConversion() {
    String jsonString = "{\"bar_dt\":\"20201118000000\"}\n";
    SchemaAndValue sav = converter.toConnectData("topic", jsonString.getBytes());
    Struct struct = (Struct)sav.value();
    Assert.assertEquals(Date.class.getCanonicalName(), struct.get("bar_dt").getClass().getCanonicalName());
  }

  @Test
  public void testJsonStringAsTimestampConversion() {
    String jsonString = "{\"bar_ts\":\"20200203090732000000\"}\n";
    SchemaAndValue sav = converter.toConnectData("topic", jsonString.getBytes());
    Struct struct = (Struct)sav.value();
    Assert.assertEquals(Date.class.getCanonicalName(), struct.get("bar_ts").getClass().getCanonicalName());
  }

  @Test
  public void testJsonStringAsDateWithoutDTTag() {

    Map<String, Object> objectConfig = new HashMap<>();
    objectConfig.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, false);
    objectConfig.put(JsonConverterConfig.DT_ATTRS_CONFIG, Arrays.asList("foo"));

    JSONConverter converter = new JSONConverter();
    converter.configure(objectConfig, false);

    String jsonString = "{\"foo\":\"20201118000000\"}\n";
    SchemaAndValue sav = converter.toConnectData("topic", jsonString.getBytes());
    Struct struct = (Struct)sav.value();
    Assert.assertEquals(Date.class.getCanonicalName(), struct.get("foo").getClass().getCanonicalName());
    Assert.assertEquals(2020, ((Date)struct.get("foo")).getYear()+1900);
    Assert.assertEquals(11, ((Date)struct.get("foo")).getMonth()+1);
    Assert.assertEquals(18, ((Date)struct.get("foo")).getDate());
  }

  @Test
  public void testJsonStringAsTimestampWithoutTSTag() {

    Map<String, Object> objectConfig = new HashMap<>();
    objectConfig.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, false);
    objectConfig.put(JsonConverterConfig.TS_ATTRS_CONFIG, Arrays.asList("foo"));

    JSONConverter converter = new JSONConverter();
    converter.configure(objectConfig, false);

    String jsonString = "{\"foo\":\"20200203090732000000\"}\n";
    SchemaAndValue sav = converter.toConnectData("topic", jsonString.getBytes());
    Struct struct = (Struct)sav.value();
    Assert.assertEquals(Date.class.getCanonicalName(), struct.get("foo").getClass().getCanonicalName());
    Assert.assertEquals(2020, ((Date)struct.get("foo")).getYear()+1900);
    Assert.assertEquals(2, ((Date)struct.get("foo")).getMonth()+1);
    Assert.assertEquals(3, ((Date)struct.get("foo")).getDate());
    Assert.assertEquals(9, ((Date)struct.get("foo")).getHours());
    Assert.assertEquals(7, ((Date)struct.get("foo")).getMinutes());
    Assert.assertEquals(32, ((Date)struct.get("foo")).getSeconds());
  }

  @Test
  public void testJsonStringAsTimestampWithCustomPattern() {

    Map<String, Object> objectConfig = new HashMap<>();
    objectConfig.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, false);
    objectConfig.put(JsonConverterConfig.TS_ATTRS_CONFIG, Arrays.asList("foo"));
    objectConfig.put(JsonConverterConfig.TS_PATTERN_CONFIG, "yyyyMMddHHmmss");

    JSONConverter converter = new JSONConverter();
    converter.configure(objectConfig, false);

    String jsonString = "{\"foo\":\"20200203090732\"}\n";
    SchemaAndValue sav = converter.toConnectData("topic", jsonString.getBytes());
    Struct struct = (Struct)sav.value();
    Assert.assertEquals(Date.class.getCanonicalName(), struct.get("foo").getClass().getCanonicalName());
    Assert.assertEquals(2020, ((Date)struct.get("foo")).getYear()+1900);
    Assert.assertEquals(2, ((Date)struct.get("foo")).getMonth()+1);
    Assert.assertEquals(3, ((Date)struct.get("foo")).getDate());
    Assert.assertEquals(9, ((Date)struct.get("foo")).getHours());
    Assert.assertEquals(7, ((Date)struct.get("foo")).getMinutes());
    Assert.assertEquals(32, ((Date)struct.get("foo")).getSeconds());
  }

  @Test
  public void testJsonStringAsDateWithCustomPattern() {

    Map<String, Object> objectConfig = new HashMap<>();
    objectConfig.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, false);
    objectConfig.put(JsonConverterConfig.DT_ATTRS_CONFIG, Arrays.asList("foo"));
    objectConfig.put(JsonConverterConfig.DT_PATTERN_CONFIG, "yyyy.MM.dd");

    JSONConverter converter = new JSONConverter();
    converter.configure(objectConfig, false);

    String jsonString = "{\"foo\":\"2020.12.29\"}\n";
    SchemaAndValue sav = converter.toConnectData("topic", jsonString.getBytes());
    Struct struct = (Struct)sav.value();
    Assert.assertEquals(Date.class.getCanonicalName(), struct.get("foo").getClass().getCanonicalName());
    Assert.assertEquals(2020, ((Date)struct.get("foo")).getYear()+1900);
    Assert.assertEquals(12, ((Date)struct.get("foo")).getMonth()+1);
    Assert.assertEquals(29, ((Date)struct.get("foo")).getDate());
  }

}
