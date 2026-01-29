package com.maiload.messagereceiver.worker.application.service;

import com.maiload.messagereceiver.common.exception.ErrorCode;
import com.maiload.messagereceiver.common.exception.GatewayException;
import com.maiload.messagereceiver.common.util.IdGenerator;
import com.maiload.messagereceiver.worker.application.port.in.MessageProcessPort;
import com.maiload.messagereceiver.worker.application.port.out.CdrPublisherPort;
import com.maiload.messagereceiver.worker.application.port.out.GatewayPort;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDateTime;
import java.util.HexFormat;

@Slf4j
@Service
@RequiredArgsConstructor
public class MessageProcessService implements MessageProcessPort {

    private final GatewayPort gatewayPort;
    private final CdrPublisherPort cdrPublisherPort;

    @Override
    public void process(Process process) {
        log.info("Processing message: receiptId={}, customerId={}", process.receiptId(), process.customerId());

        GatewayPort.SendResult result;
        try {
            result = gatewayPort.send(toSend(process));
        } catch (GatewayException e) {
            cdrPublisherPort.publish(toCdrEvent(process, "FAILED", null, e.getErrorCode().getCode(), e.getMessage()));
            throw e;
        }

        String status = result.success() ? "SENT" : "FAILED";
        cdrPublisherPort.publish(toCdrEvent(process, status, result.providerMessageId(), result.failCode(), result.failReason()));

        if (!result.success()) {
            throw new GatewayException(ErrorCode.GATEWAY_4XX_PERMANENT,
                    "Gateway returned failure: " + result.failReason());
        }

        log.info("Message sent: receiptId={}, providerMessageId={}",
                process.receiptId(), result.providerMessageId());
    }

    private GatewayPort.Send toSend(Process process) {
        return new GatewayPort.Send(
                process.receiptId(),
                process.channel(),
                process.recipient(),
                process.content(),
                process.mediaUrls()
        );
    }

    private CdrPublisherPort.CdrEvent toCdrEvent(Process process, String status,
                                                   String providerMessageId, String failCode, String failReason) {
        return new CdrPublisherPort.CdrEvent(
                IdGenerator.uuid(),
                "DELIVERY_RESULT",
                LocalDateTime.now(),
                process.customerId(),
                process.receiptId(),
                process.customerMessageId(),
                process.channel(),
                status,
                hashRecipient(process.recipient()),
                1,
                0,
                providerMessageId,
                failCode,
                failReason
        );
    }

    private String hashRecipient(String recipient) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(recipient.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hash);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 algorithm not available", e);
        }
    }
}
