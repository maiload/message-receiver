package com.maiload.messagereceiver.common.exception;

public class InfrastructureException extends BaseException {

    public InfrastructureException(ErrorCode errorCode) {
        super(errorCode);
    }

    public InfrastructureException(ErrorCode errorCode, String message) {
        super(errorCode, message);
    }

    public InfrastructureException(ErrorCode errorCode, String message, Throwable cause) {
        super(errorCode, message, cause);
    }
}
