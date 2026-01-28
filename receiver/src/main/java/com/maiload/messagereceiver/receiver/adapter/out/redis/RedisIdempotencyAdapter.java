package com.maiload.messagereceiver.receiver.adapter.out.redis;

import com.maiload.messagereceiver.receiver.application.port.out.IdempotencyPort;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Optional;

@Component
@RequiredArgsConstructor
public class RedisIdempotencyAdapter implements IdempotencyPort {

    private static final String KEY_PREFIX = "idem:";

    private final StringRedisTemplate redisTemplate;

    @Value("${receiver.idempotency.ttl-seconds:86400}")
    private long ttlSeconds;

    @Override
    public Optional<String> checkAndSet(String customerId, String customerMessageId, String receiptId) {
        String key = KEY_PREFIX + customerId + ":" + customerMessageId;

        Boolean success = redisTemplate.opsForValue().setIfAbsent(key, receiptId, Duration.ofSeconds(ttlSeconds));

        if (Boolean.TRUE.equals(success)) {
            return Optional.empty();
        }

        String existingReceiptId = redisTemplate.opsForValue().get(key);
        return Optional.ofNullable(existingReceiptId);
    }
}
