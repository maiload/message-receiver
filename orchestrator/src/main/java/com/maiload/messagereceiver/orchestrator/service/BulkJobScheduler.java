package com.maiload.messagereceiver.orchestrator.service;

import static com.maiload.messagereceiver.common.domain.JobStatus.*;

import com.maiload.messagereceiver.orchestrator.repository.BulkJobRepository.PendingJob;
import com.maiload.messagereceiver.orchestrator.repository.TemplateRepository;
import io.minio.GetObjectArgs;
import io.minio.MinioClient;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import tools.jackson.databind.json.JsonMapper;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

@Slf4j
@Service
@RequiredArgsConstructor
public class BulkJobScheduler {

    private final BulkJobService bulkJobService;
    private final TemplateRepository templateRepository;
    private final MinioClient minioClient;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final JsonMapper jsonMapper;

    @Value("${orchestrator.minio.bucket}")
    private String bucket;

    @Value("${orchestrator.kafka.bulk-task-topic}")
    private String bulkTaskTopic;

    @Value("${orchestrator.job.chunk-size}")
    private int chunkSize;

    @Value("${orchestrator.job.lease-duration-seconds}")
    private int leaseDurationSeconds;

    @Value("${orchestrator.job.heartbeat-interval-ms}")
    private long heartbeatIntervalMs;

    private final String instanceId = UUID.randomUUID().toString().substring(0, 8);

    private final ScheduledExecutorService heartbeatExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "lease-heartbeat");
        t.setDaemon(true);
        return t;
    });

    @Scheduled(fixedDelayString = "${orchestrator.job.poll-interval-ms}")
    public void pollAndProcess() {
        List<PendingJob> jobs = bulkJobService.claimJobs(instanceId, leaseDurationSeconds);

        for (PendingJob job : jobs) {
            ScheduledFuture<?> heartbeat = startHeartbeat(job.jobId());
            try {
                processJob(job);
                bulkJobService.updateStatusAndReleaseLease(job.jobId(), PUBLISHED);
            } catch (Exception e) {
                log.error("Failed to process job: jobId={}", job.jobId(), e);
                bulkJobService.updateStatusAndReleaseLease(job.jobId(), FAILED);
            } finally {
                heartbeat.cancel(false);
            }
        }
    }

    private ScheduledFuture<?> startHeartbeat(String jobId) {
        return heartbeatExecutor.scheduleAtFixedRate(() -> {
            try {
                bulkJobService.renewLease(jobId, instanceId, leaseDurationSeconds);
            } catch (Exception e) {
                log.warn("Failed to renew lease: jobId={}", jobId, e);
            }
        }, heartbeatIntervalMs, heartbeatIntervalMs, TimeUnit.MILLISECONDS);
    }

    private void processJob(PendingJob job) throws Exception {
        int resumeFrom = job.publishedChunks();
        int skipLines = resumeFrom * chunkSize;

        log.info("Processing bulk job: jobId={}, objectKey={}, resumeFrom={}",
                job.jobId(), job.objectKey(), resumeFrom);

        TemplateRepository.TemplateInfo template = templateRepository
                .findByTemplateId(job.templateId())
                .orElseThrow(() -> new IllegalStateException(
                        "Template not found: " + job.templateId()));

        int totalLines = 0;
        int chunkIndex = resumeFrom;
        int linesInCurrentChunk = 0;

        try (InputStream is = minioClient.getObject(
                GetObjectArgs.builder().bucket(bucket).object(job.objectKey()).build());
             BufferedReader reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(is)))) {

            while (reader.readLine() != null) {
                totalLines++;

                if (totalLines <= skipLines) continue;

                linesInCurrentChunk++;

                if (linesInCurrentChunk == chunkSize) {
                    int startOffset = chunkIndex * chunkSize;
                    publishChunk(job, template, chunkIndex, startOffset, startOffset + chunkSize);
                    chunkIndex++;
                    bulkJobService.updatePublishedChunks(job.jobId(), chunkIndex);
                    linesInCurrentChunk = 0;
                }
            }

            if (linesInCurrentChunk > 0) {
                int startOffset = chunkIndex * chunkSize;
                publishChunk(job, template, chunkIndex, startOffset, startOffset + linesInCurrentChunk);
                chunkIndex++;
                bulkJobService.updatePublishedChunks(job.jobId(), chunkIndex);
            }
        }

        bulkJobService.updateTotalCount(job.jobId(), totalLines);

        log.info("Bulk job published: jobId={}, totalLines={}, chunks={}",
                job.jobId(), totalLines, chunkIndex);
    }

    private void publishChunk(PendingJob job, TemplateRepository.TemplateInfo template,
                              int chunkIndex, int startOffset, int endOffset) throws Exception {
        BulkSendTask task = new BulkSendTask(
                job.jobId(),
                chunkIndex,
                job.customerId(),
                job.templateId(),
                template.channel(),
                template.content(),
                new BulkSendTask.DatasetRef(job.objectKey(), startOffset, endOffset)
        );

        String json = jsonMapper.writeValueAsString(task);
        kafkaTemplate.send(bulkTaskTopic, job.jobId(), json).get();

        log.debug("Published bulk task: jobId={}, chunkIndex={}, offset={}-{}",
                job.jobId(), chunkIndex, startOffset, endOffset);
    }

    record BulkSendTask(
            String jobId,
            int chunkIndex,
            String customerId,
            String templateId,
            String channel,
            String contentTemplate,
            DatasetRef datasetRef
    ) {
        record DatasetRef(
                String objectKey,
                int startOffset,
                int endOffset
        ) {}
    }
}
