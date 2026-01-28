package com.maiload.messagereceiver.common.exception;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum ErrorCode {

    // 공통 에러 (1xxx)
    INTERNAL_SERVER_ERROR(1000, "내부 서버 오류가 발생했습니다"),
    INVALID_REQUEST(1001, "잘못된 요청입니다"),
    VALIDATION_FAILED(1002, "유효성 검증에 실패했습니다"),

    // 인증/인가 에러 (2xxx)
    UNAUTHORIZED(2000, "인증이 필요합니다"),
    FORBIDDEN(2001, "접근 권한이 없습니다"),
    INVALID_API_KEY(2002, "유효하지 않은 API 키입니다"),

    // Rate Limit 에러 (3xxx)
    RATE_LIMIT_EXCEEDED(3000, "요청 한도를 초과했습니다"),
    QUOTA_EXCEEDED(3001, "일일 할당량을 초과했습니다"),

    // 메시지 에러 (4xxx)
    INVALID_PHONE_NUMBER(4000, "유효하지 않은 전화번호입니다"),
    INVALID_MESSAGE_CONTENT(4001, "유효하지 않은 메시지 내용입니다"),
    MESSAGE_TOO_LONG(4002, "메시지 길이가 초과되었습니다"),
    INVALID_SENDER(4003, "유효하지 않은 발신번호입니다"),
    DUPLICATE_MESSAGE(4004, "중복된 메시지입니다"),

    // Bulk 에러 (5xxx)
    INVALID_FILE_FORMAT(5000, "유효하지 않은 파일 형식입니다"),
    FILE_TOO_LARGE(5001, "파일 크기가 초과되었습니다"),
    JOB_NOT_FOUND(5002, "작업을 찾을 수 없습니다"),
    JOB_ALREADY_STARTED(5003, "이미 시작된 작업입니다"),
    JOB_CANCELLED(5004, "취소된 작업입니다"),

    // 외부 시스템 에러 (6xxx)
    GATEWAY_ERROR(6000, "게이트웨이 연동 오류가 발생했습니다"),
    GATEWAY_TIMEOUT(6001, "게이트웨이 응답 시간이 초과되었습니다"),
    MQ_PUBLISH_FAILED(6002, "메시지 큐 발행에 실패했습니다");

    private final int code;
    private final String message;
}
