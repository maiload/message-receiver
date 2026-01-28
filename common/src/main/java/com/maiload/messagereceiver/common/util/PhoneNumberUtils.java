package com.maiload.messagereceiver.common.util;

import com.google.i18n.phonenumbers.NumberParseException;
import com.google.i18n.phonenumbers.PhoneNumberUtil;
import com.google.i18n.phonenumbers.Phonenumber.PhoneNumber;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class PhoneNumberUtils {

    private static final PhoneNumberUtil PHONE_UTIL = PhoneNumberUtil.getInstance();
    private static final String DEFAULT_REGION = "KR";

    public static boolean isValid(String phoneNumber) {
        try {
            PhoneNumber parsed = PHONE_UTIL.parse(phoneNumber, DEFAULT_REGION);
            return PHONE_UTIL.isValidNumber(parsed);
        } catch (NumberParseException e) {
            return false;
        }
    }

    public static String normalize(String phoneNumber) {
        try {
            PhoneNumber parsed = PHONE_UTIL.parse(phoneNumber, DEFAULT_REGION);
            return PHONE_UTIL.format(parsed, PhoneNumberUtil.PhoneNumberFormat.E164);
        } catch (NumberParseException e) {
            return phoneNumber;
        }
    }
}
