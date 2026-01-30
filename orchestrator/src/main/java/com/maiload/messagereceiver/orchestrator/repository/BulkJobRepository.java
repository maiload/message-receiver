package com.maiload.messagereceiver.orchestrator.repository;

import static com.maiload.messagereceiver.orchestrator.jooq.tables.BulkJobs.BULK_JOBS;

import static com.maiload.messagereceiver.common.domain.JobStatus.*;

import com.maiload.messagereceiver.common.domain.JobStatus;
import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;

@Repository
@RequiredArgsConstructor
public class BulkJobRepository {

    private final DSLContext dsl;

    private static final int MAX_RETRY = 3;

    public List<PendingJob> claimJobs(String instanceId, int leaseDurationSeconds) {
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime leaseUntil = now.plusSeconds(leaseDurationSeconds);

        // 대상 job_id 조회
        var jobIds = dsl.select(BULK_JOBS.JOB_ID)
                .from(BULK_JOBS)
                .where(
                        // PENDING/FAILED 상태
                        BULK_JOBS.STATUS.in(PENDING, FAILED)
                                .and(BULK_JOBS.RETRY_COUNT.lt(MAX_RETRY))
                                .and(BULK_JOBS.SCHEDULED_AT.isNull()
                                        .or(BULK_JOBS.SCHEDULED_AT.le(now)))
                        // 또는 PROCESSING이지만 lease 만료 (크래시 복구)
                        .or(BULK_JOBS.STATUS.eq(PROCESSING)
                                .and(BULK_JOBS.LOCKED_UNTIL.lt(now)))
                )
                .orderBy(BULK_JOBS.CREATED_AT.asc())
                .limit(10)
                .forUpdate().skipLocked()
                .fetchInto(String.class);

        if (jobIds.isEmpty()) return List.of();

        // 원자적 claim: status → PROCESSING, lease 설정
        dsl.update(BULK_JOBS)
                .set(BULK_JOBS.STATUS, PROCESSING)
                .set(BULK_JOBS.LOCKED_BY, instanceId)
                .set(BULK_JOBS.LOCKED_UNTIL, leaseUntil)
                .set(BULK_JOBS.STARTED_AT, DSL.when(BULK_JOBS.STARTED_AT.isNull(), now)
                        .otherwise(BULK_JOBS.STARTED_AT))
                .where(BULK_JOBS.JOB_ID.in(jobIds))
                .execute();

        return dsl.select(
                        BULK_JOBS.JOB_ID,
                        BULK_JOBS.CUSTOMER_ID,
                        BULK_JOBS.TEMPLATE_ID,
                        BULK_JOBS.OBJECT_KEY,
                        BULK_JOBS.PUBLISHED_CHUNKS,
                        BULK_JOBS.SCHEDULED_AT)
                .from(BULK_JOBS)
                .where(BULK_JOBS.JOB_ID.in(jobIds))
                .fetchInto(PendingJob.class);
    }

    public void renewLease(String jobId, String instanceId, int leaseDurationSeconds) {
        dsl.update(BULK_JOBS)
                .set(BULK_JOBS.LOCKED_UNTIL, LocalDateTime.now().plusSeconds(leaseDurationSeconds))
                .where(BULK_JOBS.JOB_ID.eq(jobId)
                        .and(BULK_JOBS.LOCKED_BY.eq(instanceId)))
                .execute();
    }

    public void updateStatusAndReleaseLease(String jobId, JobStatus status) {
        var update = dsl.update(BULK_JOBS)
                .set(BULK_JOBS.STATUS, status)
                .set(BULK_JOBS.LOCKED_BY, (String) null)
                .set(BULK_JOBS.LOCKED_UNTIL, (LocalDateTime) null);

        if (status == FAILED) {
            update.set(BULK_JOBS.RETRY_COUNT, BULK_JOBS.RETRY_COUNT.plus(1));
        }

        update.where(BULK_JOBS.JOB_ID.eq(jobId))
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
