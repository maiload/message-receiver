package com.maiload.messagereceiver.common.exception;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum ErrorCode {

    // Authentication
    UNAUTHENTICATED("A001", false),
    PERMISSION_DENIED("A002", false),
    API_KEY_EXPIRED("A003", false),

    // Validation
    INVALID_REQUEST("V001", false),
    INVALID_RECIPIENT("V002", false),
    INVALID_TEMPLATE("V003", false),
    TEMPLATE_NOT_FOUND("V004", false),
    RECEIPT_NOT_FOUND("V005", false),
    JOB_NOT_FOUND("V006", false),

    // Policy
    RATE_LIMIT_EXCEEDED("P001", true),
    IDEMPOTENCY_CONFLICT("P002", false),

    // Infrastructure
    REDIS_CONNECTION_FAILED("I001", true),
    MQ_PUBLISH_FAILED("I002", true),
    KAFKA_PUBLISH_FAILED("I003", true),
    DB_CONNECTION_FAILED("I004", true),

    // Gateway
    GATEWAY_TIMEOUT("G001", true),
    GATEWAY_5XX("G002", true),
    GATEWAY_4XX_PERMANENT("G003", false);

    private final String code;
    private final boolean retryable;
}
