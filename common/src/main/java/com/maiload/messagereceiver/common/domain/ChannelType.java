package com.maiload.messagereceiver.common.domain;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum ChannelType {

    SMS("SMS", 90),
    LMS("LMS", 2000),
    MMS("MMS", 2000),
    RCS("RCS", 5000),
    ALIMTALK("알림톡", 1000),
    FRIENDTALK("친구톡", 1000);

    private final String displayName;
    private final int maxLength;
}
