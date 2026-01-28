package com.maiload.messagereceiver.common.domain;

public enum MessageStatus {

    RECEIVED,       // 수신 완료
    QUEUED,         // 큐 발행 완료
    PROCESSING,     // 처리 중
    SENT,           // 발송 완료 (게이트웨이 전달)
    DELIVERED,      // 수신 확인
    FAILED,         // 발송 실패
    EXPIRED         // 만료
}
