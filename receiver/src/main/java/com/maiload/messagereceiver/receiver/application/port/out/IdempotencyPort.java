package com.maiload.messagereceiver.receiver.application.port.out;

import java.util.Optional;

public interface IdempotencyPort {

    Optional<String> checkAndSet(String customerId, String customerMessageId, String receiptId);
}
