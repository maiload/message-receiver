package com.maiload.messagereceiver.common.util;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.util.UUID;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class IdGenerator {

    public static String uuid() {
        return UUID.randomUUID().toString();
    }
}
