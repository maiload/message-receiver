package com.maiload.messagereceiver.worker.application.port.in;

import com.maiload.messagereceiver.common.domain.ChannelType;
import com.maiload.messagereceiver.common.domain.SendType;

import java.util.List;
import java.util.Map;

public interface MessageProcessPort {

    void process(Process process);

    record Process(
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
            String acceptedAt
    ) {}
}
