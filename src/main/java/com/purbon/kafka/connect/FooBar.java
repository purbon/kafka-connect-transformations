package com.purbon.kafka.connect;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FooBar {

  public static void main(String[] args) throws JsonProcessingException {

    List<String> array = Arrays.asList("foo", "bar");
    HashMap<String, Object> map = new HashMap<>();
    map.put("name", "Pere");
    map.put("age", 38);
    map.put("time", new Float(3.4));
    map.put("city", array);

    Map<String, Object> props = new HashMap<>();
    props.put("foo", "3");
    map.put("props", props);


    ObjectMapper mapper = new ObjectMapper();
    String string = mapper.writeValueAsString(map);

    System.out.println(string);

    TypeReference<HashMap<String, Object>> typeRef = new TypeReference<HashMap<String, Object>>() {};
    Map<String, Object> object = mapper.readValue(string, typeRef);

    System.out.println(object.get("city")+" "+object.get("city").getClass());
    System.out.println(object.get("age")+ " " + object.get("age").getClass());
    System.out.println(object.get("time")+ " " + object.get("time").getClass());
    System.out.println(object.get("props")+ " " + object.get("props").getClass());


  }
}
