package com.maiload.messagereceiver.common.exception;

public class PolicyViolationException extends DomainException {

    public PolicyViolationException(ErrorCode errorCode) {
        super(errorCode);
    }

    public PolicyViolationException(ErrorCode errorCode, String message) {
        super(errorCode, message);
    }
}
