package com.maiload.messagereceiver.receiver.adapter.in.rest;

import com.maiload.messagereceiver.common.exception.BaseException;
import com.maiload.messagereceiver.common.exception.ErrorCode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.Map;

@Slf4j
@RestControllerAdvice
public class RestExceptionHandler {

    @ExceptionHandler(BaseException.class)
    public ResponseEntity<Map<String, String>> handleBaseException(BaseException e) {
        log.warn("Business exception: code={}, message={}", e.getErrorCode().getCode(), e.getMessage());
        return ResponseEntity
                .status(toHttpStatus(e.getErrorCode()))
                .body(Map.of(
                        "code", e.getErrorCode().getCode(),
                        "message", e.getMessage() != null ? e.getMessage() : ""
                ));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<Map<String, String>> handleException(Exception e) {
        log.error("Unexpected exception", e);
        return ResponseEntity
                .status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of(
                        "code", "INTERNAL",
                        "message", "Internal server error"
                ));
    }

    private HttpStatus toHttpStatus(ErrorCode errorCode) {
        return switch (errorCode) {
            case UNAUTHENTICATED, API_KEY_EXPIRED -> HttpStatus.UNAUTHORIZED;
            case PERMISSION_DENIED -> HttpStatus.FORBIDDEN;
            case INVALID_REQUEST, INVALID_RECIPIENT, INVALID_TEMPLATE -> HttpStatus.BAD_REQUEST;
            case TEMPLATE_NOT_FOUND, RECEIPT_NOT_FOUND, JOB_NOT_FOUND -> HttpStatus.NOT_FOUND;
            case RATE_LIMIT_EXCEEDED -> HttpStatus.TOO_MANY_REQUESTS;
            case IDEMPOTENCY_CONFLICT -> HttpStatus.CONFLICT;
            case REDIS_CONNECTION_FAILED, MQ_PUBLISH_FAILED, KAFKA_PUBLISH_FAILED, DB_CONNECTION_FAILED,
                 GATEWAY_TIMEOUT, GATEWAY_5XX -> HttpStatus.SERVICE_UNAVAILABLE;
            default -> HttpStatus.INTERNAL_SERVER_ERROR;
        };
    }
}
