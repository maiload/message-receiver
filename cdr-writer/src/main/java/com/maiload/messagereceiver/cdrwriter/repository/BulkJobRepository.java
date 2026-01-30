package com.maiload.messagereceiver.cdrwriter.repository;

import static com.maiload.messagereceiver.cdrwriter.jooq.tables.BulkJobs.BULK_JOBS;
import static com.maiload.messagereceiver.common.domain.JobStatus.*;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;

@Slf4j
@Repository
@RequiredArgsConstructor
public class BulkJobRepository {

    private final DSLContext dsl;

    public void incrementCounts(String jobId, int successCount, int failCount) {
        dsl.update(BULK_JOBS)
                .set(BULK_JOBS.SUCCESS_COUNT, BULK_JOBS.SUCCESS_COUNT.plus(successCount))
                .set(BULK_JOBS.FAIL_COUNT, BULK_JOBS.FAIL_COUNT.plus(failCount))
                .where(BULK_JOBS.JOB_ID.eq(jobId))
                .execute();

        // 완료 여부 확인 후 상태 업데이트
        dsl.update(BULK_JOBS)
                .set(BULK_JOBS.STATUS, DELIVERED)
                .set(BULK_JOBS.COMPLETED_AT, LocalDateTime.now())
                .where(BULK_JOBS.JOB_ID.eq(jobId)
                        .and(BULK_JOBS.STATUS.eq(PUBLISHED))
                        .and(BULK_JOBS.SUCCESS_COUNT.plus(BULK_JOBS.FAIL_COUNT)
                                .greaterOrEqual(BULK_JOBS.TOTAL_COUNT)))
                .execute();

        log.debug("Updated bulk job counts: jobId={}, success+={}, fail+={}",
                jobId, successCount, failCount);
    }
}
