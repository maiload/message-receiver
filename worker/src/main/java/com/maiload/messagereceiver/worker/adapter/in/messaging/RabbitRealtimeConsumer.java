package com.maiload.messagereceiver.worker.adapter.in.messaging;

import com.maiload.messagereceiver.common.domain.ChannelType;
import com.maiload.messagereceiver.common.domain.SendType;
import com.maiload.messagereceiver.common.exception.BaseException;
import com.maiload.messagereceiver.worker.application.port.in.MessageProcessPort;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

import java.util.List;
import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class RabbitRealtimeConsumer {

    private final MessageProcessPort messageProcessPort;
    private final RabbitTemplate rabbitTemplate;
    private final JsonMapper jsonMapper;

    @Value("${worker.rabbitmq.exchange}")
    private String exchange;

    @Value("${worker.rabbitmq.dlq-routing-key}")
    private String dlqRoutingKey;

    @Value("${worker.rabbitmq.retry.routing-keys}")
    private List<String> retryRoutingKeys;

    @RabbitListener(queues = "${worker.rabbitmq.queue}", ackMode = "MANUAL")
    public void onMessage(Message message, com.rabbitmq.client.Channel channel) throws Exception {
        long deliveryTag = message.getMessageProperties().getDeliveryTag();

        try {
            MessageDto dto = jsonMapper.readValue(message.getBody(), MessageDto.class);
            log.debug("Received message: receiptId={}", dto.receiptId());

            messageProcessPort.process(toProcess(dto));
            channel.basicAck(deliveryTag, false);

        } catch (BaseException e) {
            if (e.getErrorCode().isRetryable()) {
                handleRetry(message, channel, deliveryTag, e);
            } else {
                log.warn("Non-retryable error, sending to DLQ: {}", e.getMessage());
                sendToDlq(message);
                channel.basicAck(deliveryTag, false);
            }
        } catch (Exception e) {
            handleRetry(message, channel, deliveryTag, e);
        }
    }

    private void handleRetry(Message message, com.rabbitmq.client.Channel channel,
                              long deliveryTag, Exception e) throws Exception {
        int retryCount = getRetryCount(message);

        if (retryCount < retryRoutingKeys.size()) {
            String retryKey = retryRoutingKeys.get(retryCount);
            log.warn("Retrying message (attempt {}): routingKey={}, error={}",
                    retryCount + 1, retryKey, e.getMessage());
            message.getMessageProperties().setHeader("x-retry-count", retryCount + 1);
            rabbitTemplate.send(exchange, retryKey, message);
        } else {
            log.error("Max retries exceeded, sending to DLQ", e);
            sendToDlq(message);
        }

        channel.basicAck(deliveryTag, false);
    }

    private int getRetryCount(Message message) {
        Object retryCount = message.getMessageProperties().getHeader("x-retry-count");
        return retryCount != null ? (int) retryCount : 0;
    }

    private void sendToDlq(Message message) {
        rabbitTemplate.send(exchange, dlqRoutingKey, message);
    }

    private MessageProcessPort.Process toProcess(MessageDto dto) {
        return new MessageProcessPort.Process(
                dto.receiptId(),
                dto.customerId(),
                dto.customerMessageId(),
                dto.sendType(),
                dto.channel(),
                dto.recipient(),
                dto.templateId(),
                dto.content(),
                dto.vars(),
                dto.ttlSeconds(),
                dto.mediaUrls(),
                dto.acceptedAt(),
                null
        );
    }

    private record MessageDto(
            String receiptId,
            String customerId,
            String customerMessageId,
            SendType sendType,
            ChannelType channel,
            String recipient,
            String templateId,
            String content,
            Map<String, String> vars,
            Integer ttlSeconds,
            List<String> mediaUrls,
            String acceptedAt
    ) {}
}
