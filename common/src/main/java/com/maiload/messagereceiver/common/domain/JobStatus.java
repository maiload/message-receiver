package com.maiload.messagereceiver.common.domain;

public enum JobStatus {

    PENDING,        // 대기 중
    PROCESSING,     // 청크 분할 중
    PUBLISHED,      // Kafka 발행 완료
    COMPLETED,      // 전체 성공
    PARTIALLY_COMPLETED, // 일부 성공
    FAILED,         // 전체 실패
    SKIPPED         // 전체 스킵 (중복)
}
