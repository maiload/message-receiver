package com.maiload.messagereceiver.common.exception;

public class GatewayException extends InfrastructureException {

    public GatewayException(ErrorCode errorCode, String message) {
        super(errorCode, message);
    }

    public GatewayException(ErrorCode errorCode, String message, Throwable cause) {
        super(errorCode, message, cause);
    }
}
