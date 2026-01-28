package com.maiload.messagereceiver.common.domain;

public enum JobStatus {

    PENDING,        // 대기 중 (파일 업로드 완료)
    VALIDATING,     // 검증 중
    VALIDATED,      // 검증 완료
    SCHEDULED,      // 예약됨
    PROCESSING,     // 처리 중
    PAUSED,         // 일시 중지
    COMPLETED,      // 완료
    FAILED,         // 실패
    CANCELLED       // 취소
}
