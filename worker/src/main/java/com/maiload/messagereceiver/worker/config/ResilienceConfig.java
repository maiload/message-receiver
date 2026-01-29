package com.maiload.messagereceiver.worker.config;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

@Configuration
public class ResilienceConfig {

    @Bean
    public CircuitBreakerRegistry circuitBreakerRegistry() {
        CircuitBreakerConfig gatewayConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(30))
                .slidingWindowSize(100)
                .minimumNumberOfCalls(20)
                .permittedNumberOfCallsInHalfOpenState(10)
                .build();

        return CircuitBreakerRegistry.of(gatewayConfig);
    }

    @Bean
    public CircuitBreaker gatewayCircuitBreaker(CircuitBreakerRegistry registry) {
        return registry.circuitBreaker("gateway");
    }
}
