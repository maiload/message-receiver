package com.maiload.messagereceiver.receiver.adapter.out.persistence;

import com.maiload.messagereceiver.receiver.application.port.out.BulkJobRepositoryPort;
import com.maiload.messagereceiver.receiver.jooq.tables.BulkJobs;
import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
@RequiredArgsConstructor
public class JooqBulkJobRepositoryAdapter implements BulkJobRepositoryPort {

    private static final BulkJobs BULK_JOBS = BulkJobs.BULK_JOBS;

    private final DSLContext dsl;

    @Override
    public void save(BulkJob bulkJob) {
        dsl.insertInto(BULK_JOBS,
                        BULK_JOBS.JOB_ID,
                        BULK_JOBS.CUSTOMER_ID,
                        BULK_JOBS.TEMPLATE_ID,
                        BULK_JOBS.OBJECT_KEY,
                        BULK_JOBS.STATUS,
                        BULK_JOBS.SCHEDULED_AT,
                        BULK_JOBS.CALLBACK_URL)
                .values(
                        bulkJob.jobId(),
                        bulkJob.customerId(),
                        bulkJob.templateId(),
                        bulkJob.objectKey(),
                        bulkJob.status(),
                        bulkJob.scheduledAt(),
                        bulkJob.callbackUrl())
                .execute();
    }

    @Override
    public Optional<BulkJob> findByJobId(String jobId) {
        return dsl.selectFrom(BULK_JOBS)
                .where(BULK_JOBS.JOB_ID.eq(jobId))
                .fetchOptionalInto(BulkJob.class);
    }
}
