package com.maiload.messagereceiver.receiver.application.port.out;

import com.maiload.messagereceiver.common.domain.ChannelType;
import com.maiload.messagereceiver.common.domain.SendType;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

public interface RealtimeQueuePort {

    void publish(Payload payload);

    record Payload(
            String receiptId,
            String customerId,
            String customerMessageId,
            SendType sendType,
            ChannelType channel,
            String recipient,
            String templateId,
            String content,
            Map<String, String> vars,
            Integer ttlSeconds,
            List<String> mediaUrls,
            LocalDateTime acceptedAt
    ) {}
}
