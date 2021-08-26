package com.purbon.kafka.connect.jdbc;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

public class Conversions {

    public static Instant toInstantFromMicros(long microsSinceEpoch) {
        return Instant.ofEpochSecond(
                TimeUnit.MICROSECONDS.toSeconds(microsSinceEpoch),
                TimeUnit.MICROSECONDS.toNanos(microsSinceEpoch % TimeUnit.SECONDS.toMicros(1)));
    }

    public static Timestamp toTimestampFromNanos(long nanos) {
        long millis = nanos / 1000000;
        Timestamp nanoTimestamp = new Timestamp(millis);
        nanoTimestamp.setNanos((int) (nanos % 1000000000));
        return nanoTimestamp;
    }

    public static Timestamp toTimestampFromMillis(long millis) {
        return new Timestamp(millis);
    }

}
