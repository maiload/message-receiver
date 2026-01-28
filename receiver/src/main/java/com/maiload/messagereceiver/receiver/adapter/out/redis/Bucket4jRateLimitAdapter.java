package com.maiload.messagereceiver.receiver.adapter.out.redis;

import com.maiload.messagereceiver.receiver.application.port.out.RateLimitPort;
import io.github.bucket4j.Bucket;
import io.github.bucket4j.BucketConfiguration;
import io.github.bucket4j.distributed.ExpirationAfterWriteStrategy;
import io.github.bucket4j.redis.lettuce.Bucket4jLettuce;
import io.github.bucket4j.redis.lettuce.cas.LettuceBasedProxyManager;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
public class Bucket4jRateLimitAdapter implements RateLimitPort {

    private static final String KEY_PREFIX = "rl:";

    @Value("${spring.data.redis.host:localhost}")
    private String redisHost;

    @Value("${spring.data.redis.port:6379}")
    private int redisPort;

    @Value("${receiver.rate-limit.default-tps:100}")
    private int defaultTps;

    private RedisClient redisClient;
    private StatefulRedisConnection<String, byte[]> connection;
    private LettuceBasedProxyManager<String> proxyManager;

    @PostConstruct
    public void init() {
        RedisURI redisUri = RedisURI.Builder.redis(redisHost, redisPort).build();
        redisClient = RedisClient.create(redisUri);
        connection = redisClient.connect(RedisCodec.of(StringCodec.UTF8, ByteArrayCodec.INSTANCE));

        proxyManager = Bucket4jLettuce.casBasedBuilder(connection)
                .expirationAfterWrite(ExpirationAfterWriteStrategy.basedOnTimeForRefillingBucketUpToMax(Duration.ofMinutes(1)))
                .build();
    }

    @Override
    public boolean tryConsume(String customerId, int tokens) {
        String key = KEY_PREFIX + customerId;

        BucketConfiguration configuration = BucketConfiguration.builder()
                .addLimit(limit -> limit.capacity(defaultTps * 2L).refillGreedy(defaultTps, Duration.ofSeconds(1)))
                .build();

        Bucket bucket = proxyManager.getProxy(key, () -> configuration);
        return bucket.tryConsume(tokens);
    }

    @PreDestroy
    public void destroy() {
        if (connection != null) {
            connection.close();
        }
        if (redisClient != null) {
            redisClient.shutdown();
        }
    }
}
