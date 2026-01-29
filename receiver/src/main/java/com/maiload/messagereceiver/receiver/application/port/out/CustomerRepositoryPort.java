package com.maiload.messagereceiver.receiver.application.port.out;

import java.time.LocalDateTime;
import java.util.Optional;

public interface CustomerRepositoryPort {

    Optional<Customer> findByApiKeyHash(String apiKeyHash);

    record Customer(
            String customerId,
            String name,
            LocalDateTime apiKeyExpiresAt,
            int rateLimitRps,
            int rateLimitBurst,
            String status
    ) {}
}
