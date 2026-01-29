package com.maiload.messagereceiver.common.exception;

public class AuthenticationException extends DomainException {

    public AuthenticationException(ErrorCode errorCode) {
        super(errorCode);
    }

    public AuthenticationException(ErrorCode errorCode, String message) {
        super(errorCode, message);
    }
}
