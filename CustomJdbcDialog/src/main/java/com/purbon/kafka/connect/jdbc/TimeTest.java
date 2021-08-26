package com.purbon.kafka.connect.jdbc;

import java.sql.Timestamp;

public class TimeTest {

    public static void main(String[] args) {
        long epochInNanos = 1629982148392236523L;
        Timestamp timestamp = Conversions.toTimestampFromNanos(epochInNanos);
        System.out.println(epochInNanos);
        System.out.println(timestamp);

    }
}
