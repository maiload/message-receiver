package com.maiload.messagereceiver.receiver.application.service;

import static com.maiload.messagereceiver.common.domain.JobStatus.*;

import com.maiload.messagereceiver.common.exception.DomainException;
import com.maiload.messagereceiver.common.exception.ErrorCode;
import com.maiload.messagereceiver.common.util.IdGenerator;
import com.maiload.messagereceiver.receiver.application.port.in.BulkJobPort;
import com.maiload.messagereceiver.receiver.application.port.out.BulkJobRepositoryPort;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Service
@RequiredArgsConstructor
public class BulkJobService implements BulkJobPort {

    private final BulkJobRepositoryPort bulkJobRepositoryPort;

    @Override
    public CreateResult create(Create create) {
        String jobId = IdGenerator.uuid();
        LocalDateTime now = LocalDateTime.now();

        bulkJobRepositoryPort.save(new BulkJobRepositoryPort.BulkJob(
                jobId,
                create.customerId(),
                create.templateId(),
                create.objectKey(),
                PENDING,
                0, 0, 0,
                create.scheduledAt(),
                create.callbackUrl(),
                now, null, null
        ));

        return new CreateResult(jobId, now);
    }

    @Override
    public JobDetail getStatus(String customerId, String jobId) {
        BulkJobRepositoryPort.BulkJob job = bulkJobRepositoryPort.findByJobId(jobId)
                .orElseThrow(() -> new DomainException(ErrorCode.JOB_NOT_FOUND, "Job not found: " + jobId));

        if (!job.customerId().equals(customerId)) {
            throw new DomainException(ErrorCode.PERMISSION_DENIED, "Job does not belong to customer");
        }

        return new JobDetail(
                job.jobId(),
                job.customerId(),
                job.status(),
                job.totalCount(),
                job.successCount(),
                job.failCount(),
                job.createdAt(),
                job.startedAt(),
                job.completedAt()
        );
    }
}
