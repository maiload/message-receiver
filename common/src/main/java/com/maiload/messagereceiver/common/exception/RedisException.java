package com.maiload.messagereceiver.common.exception;

public class RedisException extends InfrastructureException {

    public RedisException(String message) {
        super(ErrorCode.REDIS_CONNECTION_FAILED, message);
    }

    public RedisException(String message, Throwable cause) {
        super(ErrorCode.REDIS_CONNECTION_FAILED, message, cause);
    }
}
