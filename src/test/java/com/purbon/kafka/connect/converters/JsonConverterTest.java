package com.purbon.kafka.connect.converters;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Date;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.junit.Assert;
import org.junit.Test;

public class JsonConverterTest {


  ObjectMapper mapper = new ObjectMapper();
  JSONConverter converter = new JSONConverter();

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

}
