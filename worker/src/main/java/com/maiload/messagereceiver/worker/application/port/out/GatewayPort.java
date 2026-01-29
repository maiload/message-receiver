package com.maiload.messagereceiver.worker.application.port.out;

import java.util.List;
import java.util.Map;

public interface GatewayPort {

    SendResult send(Send send);

    record Send(
            String receiptId,
            String channel,
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
