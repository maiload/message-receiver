package com.maiload.messagereceiver.cdrwriter.service;

import com.maiload.messagereceiver.cdrwriter.repository.BulkJobRepository;
import com.maiload.messagereceiver.cdrwriter.repository.CdrRecordRepository;
import com.maiload.messagereceiver.common.domain.ChannelType;
import com.maiload.messagereceiver.common.domain.MessageStatus;
import com.maiload.messagereceiver.common.domain.SendType;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class CdrBatchService {

    private final CdrRecordRepository cdrRecordRepository;
    private final BulkJobRepository bulkJobRepository;

    @Transactional
    public void insertBatch(List<CdrEvent> events) {
        int inserted = cdrRecordRepository.batchInsert(events);
        log.debug("Batch insert result: total={}, inserted={}, duplicates={}",
                events.size(), inserted, events.size() - inserted);

        updateBulkJobCounts(events);
    }

    private void updateBulkJobCounts(List<CdrEvent> events) {
        Map<String, List<CdrEvent>> byJobId = events.stream()
                .filter(e -> e.sendType() == SendType.BULK && e.jobId() != null)
                .collect(Collectors.groupingBy(CdrEvent::jobId));

        for (var entry : byJobId.entrySet()) {
            String jobId = entry.getKey();
            List<CdrEvent> jobEvents = entry.getValue();

            int successCount = (int) jobEvents.stream()
                    .filter(e -> e.status() == MessageStatus.SENT).count();
            int failCount = jobEvents.size() - successCount;

            bulkJobRepository.incrementCounts(jobId, successCount, failCount);
        }
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
            String providerMessageId,
            String failCode,
            String failReason,
            String jobId
    ) {}
}
