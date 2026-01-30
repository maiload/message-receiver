package com.maiload.messagereceiver.receiver.application.port.out;

import com.maiload.messagereceiver.common.domain.JobStatus;

import java.time.LocalDateTime;
import java.util.Optional;

public interface BulkJobRepositoryPort {

    void save(BulkJob bulkJob);

    Optional<BulkJob> findByJobId(String jobId);

    record BulkJob(
            String jobId,
            String customerId,
            String templateId,
            String objectKey,
            JobStatus status,
            int totalCount,
            int successCount,
            int failCount,
            int skipCount,
            LocalDateTime scheduledAt,
            LocalDateTime createdAt,
            LocalDateTime startedAt,
            LocalDateTime completedAt
    ) {}
}
