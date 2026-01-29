package com.maiload.messagereceiver.cdrwriter.consumer;

import com.maiload.messagereceiver.cdrwriter.service.CdrBatchService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class CdrEventConsumer {

    private final CdrBatchService cdrBatchService;
    private final JsonMapper jsonMapper;

    @KafkaListener(topics = "${cdr-writer.kafka.topic}")
    public void onMessage(List<String> messages) {
        List<CdrBatchService.CdrEvent> events = messages.stream()
                .map(msg -> jsonMapper.readValue(msg, CdrBatchService.CdrEvent.class))
                .toList();

        cdrBatchService.insertBatch(events);
        log.info("Processed CDR batch: size={}", events.size());
    }
}
