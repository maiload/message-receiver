package com.maiload.messagereceiver.receiver.application.port.out;

import com.maiload.messagereceiver.common.domain.MessageStatus;

import java.time.LocalDateTime;
import java.util.Optional;

public interface CdrRecordRepositoryPort {

    Optional<CdrRecord> findByCustomerIdAndReceiptId(String customerId, String receiptId);

    record CdrRecord(
            String receiptId,
            String customerMessageId,
            MessageStatus status,
            String failCode,
            String failReason,
            LocalDateTime acceptedAt,
            LocalDateTime sentAt
    ) {}
}
