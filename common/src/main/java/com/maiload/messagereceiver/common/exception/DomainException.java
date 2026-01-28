package com.maiload.messagereceiver.common.exception;

public class DomainException extends BaseException {

    public DomainException(ErrorCode errorCode) {
        super(errorCode);
    }

    public DomainException(ErrorCode errorCode, String message) {
        super(errorCode, message);
    }
}
