package com.maiload.messagereceiver.receiver.application.port.out;

import java.time.Instant;
import java.util.List;
import java.util.Map;

public interface RealtimeQueuePort {

    void publish(Payload payload);

    record Payload(
            String receiptId,
            String customerId,
            String customerMessageId,
            String channel,
            String recipient,
            String templateId,
            String content,
            Map<String, String> vars,
            Integer ttlSeconds,
            List<String> mediaUrls,
            Instant acceptedAt
    ) {}
}
