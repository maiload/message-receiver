package com.maiload.messagereceiver.worker.adapter.out.gateway;

import com.maiload.messagereceiver.common.exception.ErrorCode;
import com.maiload.messagereceiver.common.exception.GatewayException;
import com.maiload.messagereceiver.common.util.IdGenerator;
import com.maiload.messagereceiver.worker.application.port.out.GatewayPort;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.timelimiter.annotation.TimeLimiter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class HttpGatewayAdapter implements GatewayPort {

    @Override
    @CircuitBreaker(name = "gateway")
    public SendResult send(Send send) {
        log.info("[Mock Gateway] Sending message: receiptId={}, channel={}, recipient={}",
                send.receiptId(), send.channel(), send.recipient());

        try {
            Thread.sleep(50);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new GatewayException(ErrorCode.GATEWAY_TIMEOUT, "Gateway call interrupted");
        }

        // Gateway가 없기 때문에 항상 성공
        return new SendResult(
                IdGenerator.uuid(),
                true,
                null,
                null
        );
    }
}
