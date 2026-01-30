package com.maiload.messagereceiver.orchestrator.repository;

import static com.maiload.messagereceiver.orchestrator.jooq.tables.BulkJobs.BULK_JOBS;

import static com.maiload.messagereceiver.common.domain.JobStatus.*;

import com.maiload.messagereceiver.common.domain.JobStatus;
import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;

@Repository
@RequiredArgsConstructor
public class BulkJobRepository {

    private final DSLContext dsl;

    private static final int MAX_RETRY = 3;

    public List<PendingJob> findPendingJobs() {
        return dsl.select(
                        BULK_JOBS.JOB_ID,
                        BULK_JOBS.CUSTOMER_ID,
                        BULK_JOBS.TEMPLATE_ID,
                        BULK_JOBS.OBJECT_KEY,
                        BULK_JOBS.PUBLISHED_CHUNKS,
                        BULK_JOBS.SCHEDULED_AT)
                .from(BULK_JOBS)
                .where(BULK_JOBS.STATUS.in(PENDING, FAILED)
                        .and(BULK_JOBS.RETRY_COUNT.lt(MAX_RETRY))
                        .and(BULK_JOBS.SCHEDULED_AT.isNull()
                                .or(BULK_JOBS.SCHEDULED_AT.le(LocalDateTime.now()))))
                .orderBy(BULK_JOBS.CREATED_AT.asc())
                .limit(10)
                .fetchInto(PendingJob.class);
    }

    public void updateStatus(String jobId, JobStatus status) {
        dsl.update(BULK_JOBS)
                .set(BULK_JOBS.STATUS, status)
                .set(BULK_JOBS.STARTED_AT, LocalDateTime.now())
                .where(BULK_JOBS.JOB_ID.eq(jobId))
                .execute();
    }

    public void incrementRetryCount(String jobId) {
        dsl.update(BULK_JOBS)
                .set(BULK_JOBS.RETRY_COUNT, BULK_JOBS.RETRY_COUNT.plus(1))
                .where(BULK_JOBS.JOB_ID.eq(jobId))
                .execute();
    }

    public void updateTotalCount(String jobId, int totalCount) {
        dsl.update(BULK_JOBS)
                .set(BULK_JOBS.TOTAL_COUNT, totalCount)
                .where(BULK_JOBS.JOB_ID.eq(jobId))
                .execute();
    }

    public void updatePublishedChunks(String jobId, int publishedChunks) {
        dsl.update(BULK_JOBS)
                .set(BULK_JOBS.PUBLISHED_CHUNKS, publishedChunks)
                .where(BULK_JOBS.JOB_ID.eq(jobId))
                .execute();
    }

    public record PendingJob(
            String jobId,
            String customerId,
            String templateId,
            String objectKey,
            int publishedChunks,
            LocalDateTime scheduledAt
    ) {}
}
