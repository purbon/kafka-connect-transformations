package com.purbon.kafka.connect;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class FooBar {

  public static void main(String[] args) throws ParseException {

    String timestampAsString = "20201118000000";
    String pattern = "YYYYMMDDHHmmss";

    SimpleDateFormat dateFormat = new SimpleDateFormat(pattern);
    Date parsedDate = dateFormat.parse(timestampAsString);
    Timestamp timestamp = new java.sql.Timestamp(parsedDate.getTime());


    System.out.println(parsedDate);
  }

}
