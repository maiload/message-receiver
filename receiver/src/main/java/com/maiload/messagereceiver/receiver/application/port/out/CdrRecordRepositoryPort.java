package com.maiload.messagereceiver.receiver.application.port.out;

import java.time.LocalDateTime;
import java.util.Optional;

public interface CdrRecordRepositoryPort {

    Optional<CdrRecord> findByCustomerIdAndReceiptId(String customerId, String receiptId);

    record CdrRecord(
            String receiptId,
            String customerMessageId,
            String status,
            String failCode,
            String failReason,
            LocalDateTime acceptedAt,
            LocalDateTime sentAt,
            LocalDateTime finalizedAt
    ) {}
}
