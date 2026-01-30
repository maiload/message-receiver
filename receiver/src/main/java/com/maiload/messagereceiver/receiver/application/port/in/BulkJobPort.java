package com.maiload.messagereceiver.receiver.application.port.in;

import com.maiload.messagereceiver.common.domain.JobStatus;

import java.time.LocalDateTime;

public interface BulkJobPort {

    CreateResult create(Create create);

    JobDetail getStatus(String customerId, String jobId);

    record Create(
            String customerId,
            String templateId,
            String objectKey,
            LocalDateTime scheduledAt
    ) {}

    record CreateResult(
            String jobId,
            LocalDateTime createdAt
    ) {}

    record JobDetail(
            String jobId,
            String customerId,
            JobStatus status,
            int totalCount,
            int successCount,
            int failCount,
            LocalDateTime createdAt,
            LocalDateTime startedAt,
            LocalDateTime completedAt
    ) {}
}
