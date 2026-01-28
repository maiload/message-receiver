package com.maiload.messagereceiver.receiver.application.port.in;

import java.time.Instant;
import java.util.List;
import java.util.Map;

public interface RealtimeMessagePort {

    SubmitResult submit(Submit submit);

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
            Instant acceptedAt,
            boolean idempotencyHit
    ) {}
}
