package com.maiload.messagereceiver.common.domain;

public enum JobStatus {

    PENDING,        // 대기 중
    PROCESSING,     // 청크 분할 중
    PUBLISHED,      // Kafka 발행 완료
    DELIVERED,      // 발송 완료
    FAILED          // 실패
}
