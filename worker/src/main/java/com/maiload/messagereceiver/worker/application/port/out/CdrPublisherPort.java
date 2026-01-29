package com.maiload.messagereceiver.worker.application.port.out;

import java.time.Instant;

public interface CdrPublisherPort {

    void publish(CdrEvent event);

    record CdrEvent(
            String eventId,
            String eventType,
            Instant occurredAt,
            String customerId,
            String receiptId,
            String customerMessageId,
            String channel,
            String status,
            String recipientHash,
            int segments,
            long price,
            String providerMessageId,
            String failCode,
            String failReason
    ) {}
}
