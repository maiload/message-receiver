package com.maiload.messagereceiver.cdrwriter.repository;

import static com.maiload.messagereceiver.cdrwriter.jooq.tables.BulkJobs.BULK_JOBS;
import static com.maiload.messagereceiver.common.domain.JobStatus.*;

import com.maiload.messagereceiver.common.domain.JobStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.impl.DSL;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;

@Slf4j
@Repository
@RequiredArgsConstructor
public class BulkJobRepository {

    private final DSLContext dsl;

    public void incrementCounts(String jobId, int successCount, int failCount, int skipCount) {
        Field<Integer> newSuccessCount = BULK_JOBS.SUCCESS_COUNT.plus(successCount);
        Field<Integer> newFailCount = BULK_JOBS.FAIL_COUNT.plus(failCount);
        Field<Integer> newSkipCount = BULK_JOBS.SKIP_COUNT.plus(skipCount);

        Field<Integer> totalProcessed = newSuccessCount.plus(newFailCount).plus(newSkipCount);
        var isComplete = totalProcessed.greaterOrEqual(BULK_JOBS.TOTAL_COUNT);

        // 완료 시 상태 판정: 전체 성공 / 일부 성공 / 전체 실패 / 전체 스킵
        Field<JobStatus> statusExpr = DSL
                .when(isComplete.and(newSkipCount.eq(BULK_JOBS.TOTAL_COUNT)),
                        DSL.val(SKIPPED, BULK_JOBS.STATUS))
                .when(isComplete.and(newFailCount.plus(newSkipCount).eq(BULK_JOBS.TOTAL_COUNT)),
                        DSL.val(FAILED, BULK_JOBS.STATUS))
                .when(isComplete.and(newSuccessCount.eq(BULK_JOBS.TOTAL_COUNT)),
                        DSL.val(COMPLETED, BULK_JOBS.STATUS))
                .when(isComplete,
                        DSL.val(PARTIALLY_COMPLETED, BULK_JOBS.STATUS))
                .otherwise(BULK_JOBS.STATUS);

        Field<LocalDateTime> completedAtExpr = DSL
                .when(isComplete, DSL.val(LocalDateTime.now()))
                .otherwise(BULK_JOBS.COMPLETED_AT);

        dsl.update(BULK_JOBS)
                .set(BULK_JOBS.SUCCESS_COUNT, newSuccessCount)
                .set(BULK_JOBS.FAIL_COUNT, newFailCount)
                .set(BULK_JOBS.SKIP_COUNT, newSkipCount)
                .set(BULK_JOBS.STATUS, statusExpr)
                .set(BULK_JOBS.COMPLETED_AT, completedAtExpr)
                .where(BULK_JOBS.JOB_ID.eq(jobId))
                .execute();

        log.debug("Updated bulk job counts: jobId={}, success+={}, fail+={}, skip+={}",
                jobId, successCount, failCount, skipCount);
    }
}
