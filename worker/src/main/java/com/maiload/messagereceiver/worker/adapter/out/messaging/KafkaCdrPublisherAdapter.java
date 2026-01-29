package com.maiload.messagereceiver.worker.adapter.out.messaging;

import com.maiload.messagereceiver.common.exception.ErrorCode;
import com.maiload.messagereceiver.common.exception.MessagingException;
import com.maiload.messagereceiver.worker.application.port.out.CdrPublisherPort;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaCdrPublisherAdapter implements CdrPublisherPort {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final JsonMapper jsonMapper;

    @Value("${worker.kafka.cdr-topic}")
    private String cdrTopic;

    @Override
    public void publish(CdrEvent event) {
        try {
            String json = jsonMapper.writeValueAsString(event);
            kafkaTemplate.send(cdrTopic, event.customerId(), json).get();

            log.debug("Published CDR event: eventId={}, receiptId={}", event.eventId(), event.receiptId());
        } catch (Exception e) {
            log.error("Failed to publish CDR event: eventId={}", event.eventId(), e);
            throw new MessagingException(ErrorCode.KAFKA_PUBLISH_FAILED, "Failed to publish CDR event", e);
        }
    }
}
