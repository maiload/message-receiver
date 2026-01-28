package com.maiload.messagereceiver.common.domain;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum ChannelType {

    SMS("SMS", 90),
    LMS("LMS", 2000),
    MMS("MMS", 2000);

    private final String displayName;
    private final int maxLength;
}
