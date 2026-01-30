package com.maiload.messagereceiver.worker.application.port.out;

import com.maiload.messagereceiver.common.domain.ChannelType;

import java.util.List;

public interface GatewayPort {

    SendResult send(Send send);

    record Send(
            String receiptId,
            ChannelType channel,
            String recipient,
            String content,
            List<String> mediaUrls
    ) {}

    record SendResult(
            String providerMessageId,
            boolean success,
            String failCode,
            String failReason
    ) {}
}
