package com.maiload.messagereceiver.worker.application.port.out;

import com.maiload.messagereceiver.common.domain.ChannelType;
import com.maiload.messagereceiver.common.domain.MessageStatus;
import com.maiload.messagereceiver.common.domain.SendType;

import java.time.LocalDateTime;

public interface CdrPublisherPort {

    void publish(CdrEvent event);

    record CdrEvent(
            String eventId,
            String eventType,
            LocalDateTime occurredAt,
            String customerId,
            String receiptId,
            String customerMessageId,
            SendType sendType,
            ChannelType channel,
            MessageStatus status,
            String recipientHash,
            int segments,
            long price,
            String providerMessageId,
            String failCode,
            String failReason,
            String jobId
    ) {}
}
