package com.maiload.messagereceiver.receiver.application.service;

import static com.maiload.messagereceiver.common.domain.SendType.*;

import com.maiload.messagereceiver.common.domain.ChannelType;
import com.maiload.messagereceiver.common.exception.DomainException;
import com.maiload.messagereceiver.common.exception.ErrorCode;
import com.maiload.messagereceiver.common.exception.PolicyViolationException;
import com.maiload.messagereceiver.common.exception.ValidationException;
import com.maiload.messagereceiver.common.util.IdGenerator;
import com.maiload.messagereceiver.common.util.PhoneNumberUtils;
import com.maiload.messagereceiver.receiver.application.port.in.RealtimeMessagePort;
import com.maiload.messagereceiver.receiver.application.port.out.CdrRecordRepositoryPort;
import com.maiload.messagereceiver.receiver.application.port.out.IdempotencyPort;
import com.maiload.messagereceiver.receiver.application.port.out.RateLimitPort;
import com.maiload.messagereceiver.receiver.application.port.out.RealtimeQueuePort;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.time.LocalDateTime;
import java.util.Optional;

@Service
@RequiredArgsConstructor
public class RealtimeMessageService implements RealtimeMessagePort {

    private final IdempotencyPort idempotencyPort;
    private final RateLimitPort rateLimitPort;
    private final RealtimeQueuePort realtimeQueuePort;
    private final CdrRecordRepositoryPort cdrRecordRepositoryPort;

    @Override
    public SubmitResult submit(Submit submit) {
        validate(submit);

        if (!rateLimitPort.tryConsume(submit.customerId(), 1)) {
            throw new PolicyViolationException(ErrorCode.RATE_LIMIT_EXCEEDED);
        }

        String receiptId = IdGenerator.uuid();
        Optional<String> existingReceiptId = idempotencyPort.checkAndSet(
                submit.customerId(),
                submit.customerMessageId(),
                receiptId
        );

        if (existingReceiptId.isPresent()) {
            return new SubmitResult(existingReceiptId.get(), LocalDateTime.now(), true);
        }

        LocalDateTime acceptedAt = LocalDateTime.now();
        ChannelType channel = ChannelType.valueOf(submit.messageType().toUpperCase());
        RealtimeQueuePort.Payload payload = new RealtimeQueuePort.Payload(
                receiptId,
                submit.customerId(),
                submit.customerMessageId(),
                REALTIME,
                channel,
                PhoneNumberUtils.normalize(submit.recipient()),
                submit.templateId(),
                submit.content(),
                submit.vars(),
                submit.ttlSeconds(),
                submit.mediaUrls(),
                acceptedAt
        );

        realtimeQueuePort.publish(payload);

        return new SubmitResult(receiptId, acceptedAt, false);
    }

    @Override
    public ReceiptStatus getReceiptStatus(String customerId, String receiptId) {
        CdrRecordRepositoryPort.CdrRecord record = cdrRecordRepositoryPort
                .findByCustomerIdAndReceiptId(customerId, receiptId)
                .orElseThrow(() -> new DomainException(ErrorCode.RECEIPT_NOT_FOUND, "Receipt not found: " + receiptId));

        return new ReceiptStatus(
                record.receiptId(),
                record.customerMessageId(),
                record.status(),
                record.failCode(),
                record.failReason(),
                record.acceptedAt(),
                record.sentAt()
        );
    }

    private void validate(Submit submit) {
        if (!StringUtils.hasText(submit.customerId())) {
            throw new ValidationException(ErrorCode.INVALID_REQUEST, "customerId is required");
        }
        if (!StringUtils.hasText(submit.customerMessageId())) {
            throw new ValidationException(ErrorCode.INVALID_REQUEST, "customerMessageId is required");
        }
        if (!StringUtils.hasText(submit.messageType()) || !isValidChannelType(submit.messageType())) {
            throw new ValidationException(ErrorCode.INVALID_REQUEST, "Invalid messageType");
        }
        if (!StringUtils.hasText(submit.recipient()) || !PhoneNumberUtils.isValid(submit.recipient())) {
            throw new ValidationException(ErrorCode.INVALID_RECIPIENT, "Invalid recipient phone number");
        }
    }

    private boolean isValidChannelType(String messageType) {
        try {
            ChannelType.valueOf(messageType.toUpperCase());
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }
}
