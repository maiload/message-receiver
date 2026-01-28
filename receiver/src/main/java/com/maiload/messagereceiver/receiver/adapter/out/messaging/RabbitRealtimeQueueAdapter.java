package com.maiload.messagereceiver.receiver.adapter.out.messaging;

import org.springframework.amqp.core.Message;
import tools.jackson.databind.json.JsonMapper;
import com.maiload.messagereceiver.common.exception.ErrorCode;
import com.maiload.messagereceiver.common.exception.MessagingException;
import com.maiload.messagereceiver.receiver.application.port.out.RealtimeQueuePort;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class RabbitRealtimeQueueAdapter implements RealtimeQueuePort {

    private final RabbitTemplate rabbitTemplate;
    private final JsonMapper jsonMapper;

    @Value("${receiver.rabbitmq.exchange}")
    private String exchange;

    @Value("${receiver.rabbitmq.routing-key}")
    private String routingKey;

    @Override
    public void publish(Payload payload) {
        try {
            byte[] body = jsonMapper.writeValueAsBytes(toDto(payload));

            MessageProperties props = new MessageProperties();
            props.setContentType(MessageProperties.CONTENT_TYPE_JSON);
            props.setMessageId(payload.receiptId());
            props.setHeader("customerId", payload.customerId());

            Message mqMessage = new Message(body, props);
            rabbitTemplate.send(exchange, routingKey, mqMessage);

            log.debug("Published message to RabbitMQ: receiptId={}", payload.receiptId());
        } catch (Exception e) {
            log.error("Failed to publish message to RabbitMQ: receiptId={}", payload.receiptId(), e);
            throw new MessagingException(ErrorCode.MQ_PUBLISH_FAILED, "Failed to publish message", e);
        }
    }

    private RealtimeMessageDto toDto(Payload payload) {
        return new RealtimeMessageDto(
                payload.receiptId(),
                payload.customerId(),
                payload.customerMessageId(),
                payload.channel(),
                payload.recipient(),
                payload.templateId(),
                payload.content(),
                payload.vars(),
                payload.ttlSeconds(),
                payload.mediaUrls(),
                payload.acceptedAt().toString()
        );
    }

    private record RealtimeMessageDto(
            String receiptId,
            String customerId,
            String customerMessageId,
            String channel,
            String recipient,
            String templateId,
            String content,
            Map<String, String> vars,
            Integer ttlSeconds,
            List<String> mediaUrls,
            String acceptedAt
    ) {}
}
