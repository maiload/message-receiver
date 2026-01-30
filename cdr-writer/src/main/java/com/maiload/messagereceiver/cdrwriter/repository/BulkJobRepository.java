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

    public void incrementCounts(String jobId, int successCount, int failCount) {
        Field<Integer> newSuccessCount = BULK_JOBS.SUCCESS_COUNT.plus(successCount);
        Field<Integer> newFailCount = BULK_JOBS.FAIL_COUNT.plus(failCount);

        Field<JobStatus> statusExpr = DSL
                .when(newSuccessCount.plus(newFailCount).greaterOrEqual(BULK_JOBS.TOTAL_COUNT),
                        DSL.val(DELIVERED))
                .otherwise(BULK_JOBS.STATUS);

        Field<LocalDateTime> completedAtExpr = DSL
                .when(newSuccessCount.plus(newFailCount).greaterOrEqual(BULK_JOBS.TOTAL_COUNT),
                        DSL.val(LocalDateTime.now()))
                .otherwise(BULK_JOBS.COMPLETED_AT);

        dsl.update(BULK_JOBS)
                .set(BULK_JOBS.SUCCESS_COUNT, newSuccessCount)
                .set(BULK_JOBS.FAIL_COUNT, newFailCount)
                .set(BULK_JOBS.STATUS, statusExpr)
                .set(BULK_JOBS.COMPLETED_AT, completedAtExpr)
                .where(BULK_JOBS.JOB_ID.eq(jobId))
                .execute();

        log.debug("Updated bulk job counts: jobId={}, success+={}, fail+={}",
                jobId, successCount, failCount);
    }
}
