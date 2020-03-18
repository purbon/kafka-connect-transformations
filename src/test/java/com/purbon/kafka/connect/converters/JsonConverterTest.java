package com.purbon.kafka.connect.converters;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.SchemaAndValue;
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
}
