package com.purbon.kafka.connect;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class FooBar {

  public static void main(String[] args) throws ParseException {

    String timestampAsString = "20200316151852164214";
    String pattern = "YYYYMMDDHHmmssSSSSSSS";

    SimpleDateFormat dateFormat = new SimpleDateFormat(pattern);
    Date parsedDate = dateFormat.parse(timestampAsString);
    Timestamp timestamp = new java.sql.Timestamp(parsedDate.getTime());


    System.out.println(timestamp);
  }

}
