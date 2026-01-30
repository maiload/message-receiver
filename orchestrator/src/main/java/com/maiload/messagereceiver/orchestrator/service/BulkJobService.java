package com.maiload.messagereceiver.orchestrator.service;

import com.maiload.messagereceiver.common.domain.JobStatus;
import com.maiload.messagereceiver.orchestrator.repository.BulkJobRepository;
import com.maiload.messagereceiver.orchestrator.repository.BulkJobRepository.PendingJob;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
@RequiredArgsConstructor
public class BulkJobService {

    private final BulkJobRepository bulkJobRepository;

    @Transactional
    public List<PendingJob> claimJobs(String instanceId, int leaseDurationSeconds) {
        return bulkJobRepository.claimJobs(instanceId, leaseDurationSeconds);
    }

    public void renewLease(String jobId, String instanceId, int leaseDurationSeconds) {
        bulkJobRepository.renewLease(jobId, instanceId, leaseDurationSeconds);
    }

    public void updateStatusAndReleaseLease(String jobId, JobStatus status) {
        bulkJobRepository.updateStatusAndReleaseLease(jobId, status);
    }

    public void updatePublishedChunks(String jobId, int publishedChunks) {
        bulkJobRepository.updatePublishedChunks(jobId, publishedChunks);
    }

    public void updateTotalCount(String jobId, int totalCount) {
        bulkJobRepository.updateTotalCount(jobId, totalCount);
    }
}
