package com.maiload.messagereceiver.receiver.application.port.out;

public interface RateLimitPort {

    boolean tryConsume(String customerId, int tokens);
}
