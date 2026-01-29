package com.maiload.messagereceiver.cdrwriter.service;

import com.maiload.messagereceiver.cdrwriter.repository.CdrRecordRepository;
import com.maiload.messagereceiver.common.domain.ChannelType;
import com.maiload.messagereceiver.common.domain.MessageStatus;
import com.maiload.messagereceiver.common.domain.SendType;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class CdrBatchService {

    private final CdrRecordRepository cdrRecordRepository;

    public void insertBatch(List<CdrEvent> events) {
        int inserted = cdrRecordRepository.batchInsert(events);
        log.debug("Batch insert result: total={}, inserted={}, duplicates={}",
                events.size(), inserted, events.size() - inserted);
    }

    public record CdrEvent(
            String eventId,
            String eventType,
            LocalDateTime occurredAt,
            String customerId,
            String receiptId,
            String customerMessageId,
            SendType sendType,
            ChannelType channel,
            MessageStatus status,
            String recipientHash,
            int segments,
            long price,
            String providerMessageId,
            String failCode,
            String failReason
    ) {}
}
