package com.maiload.messagereceiver.common.exception;

public class ValidationException extends DomainException {

    public ValidationException(ErrorCode errorCode) {
        super(errorCode);
    }

    public ValidationException(ErrorCode errorCode, String message) {
        super(errorCode, message);
    }
}
