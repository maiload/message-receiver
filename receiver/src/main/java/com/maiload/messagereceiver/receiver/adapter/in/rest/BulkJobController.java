package com.maiload.messagereceiver.receiver.adapter.in.rest;

import com.maiload.messagereceiver.receiver.application.port.in.BulkJobPort;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;

@RestController
@RequestMapping("/bulk/jobs")
@RequiredArgsConstructor
public class BulkJobController {

    private final BulkJobPort bulkJobPort;

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public CreateJobResponse create(@RequestAttribute(RestAuthenticationInterceptor.CUSTOMER_ID_ATTR) String customerId,
                                    @Valid @RequestBody CreateJobRequest request) {
        BulkJobPort.CreateResult result = bulkJobPort.create(request.toCreate(customerId));
        return new CreateJobResponse(result.jobId(), result.createdAt());
    }

    @GetMapping("/{jobId}")
    public JobStatusResponse getStatus(@RequestAttribute(RestAuthenticationInterceptor.CUSTOMER_ID_ATTR) String customerId,
                                       @PathVariable String jobId) {
        BulkJobPort.JobDetail detail = bulkJobPort.getStatus(customerId, jobId);
        return JobStatusResponse.from(detail);
    }

    record CreateJobRequest(
            String templateId,
            @NotBlank String objectKey,
            LocalDateTime scheduledAt
    ) {
        BulkJobPort.Create toCreate(String customerId) {
            return new BulkJobPort.Create(customerId, templateId, objectKey, scheduledAt);
        }
    }

    record CreateJobResponse(
            String jobId,
            LocalDateTime createdAt
    ) {}

    record JobStatusResponse(
            String jobId,
            String customerId,
            String status,
            int totalCount,
            int successCount,
            int failCount,
            int pendingCount,
            LocalDateTime createdAt,
            LocalDateTime startedAt,
            LocalDateTime completedAt
    ) {
        static JobStatusResponse from(BulkJobPort.JobDetail s) {
            return new JobStatusResponse(
                    s.jobId(), s.customerId(), s.status().name(),
                    s.totalCount(), s.successCount(), s.failCount(),
                    s.totalCount() - s.successCount() - s.failCount(),
                    s.createdAt(), s.startedAt(), s.completedAt()
            );
        }
    }
}
