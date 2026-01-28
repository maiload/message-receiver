package com.maiload.messagereceiver.common.exception;

public class MessagingException extends InfrastructureException {

    public MessagingException(ErrorCode errorCode, String message) {
        super(errorCode, message);
    }

    public MessagingException(ErrorCode errorCode, String message, Throwable cause) {
        super(errorCode, message, cause);
    }
}
