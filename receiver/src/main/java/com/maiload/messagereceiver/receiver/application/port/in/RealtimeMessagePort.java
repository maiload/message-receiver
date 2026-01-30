package com.maiload.messagereceiver.receiver.application.port.in;

import com.maiload.messagereceiver.common.domain.MessageStatus;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

public interface RealtimeMessagePort {

    SubmitResult submit(Submit submit);

    ReceiptStatus getReceiptStatus(String customerId, String receiptId);

    record Submit(
            String customerId,
            String customerMessageId,
            String messageType,
            String recipient,
            String templateId,
            String content,
            Map<String, String> vars,
            Integer ttlSeconds,
            List<String> mediaUrls
    ) {}

    record SubmitResult(
            String receiptId,
            LocalDateTime acceptedAt,
            boolean idempotencyHit
    ) {}

    record ReceiptStatus(
            String receiptId,
            String customerMessageId,
            MessageStatus status,
            String failCode,
            String failReason,
            LocalDateTime acceptedAt,
            LocalDateTime sentAt
    ) {}
}
