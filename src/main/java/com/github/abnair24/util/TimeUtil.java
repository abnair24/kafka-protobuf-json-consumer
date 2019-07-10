package com.github.abnair24.util;

import java.time.Instant;

public class TimeUtil {

    public static long getCurrentTimeInMilliseconds() {
        Instant instant = Instant.now();
        return instant.toEpochMilli();
    }
}
