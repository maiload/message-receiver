package com.maiload.messagereceiver.orchestrator.service;

import static com.maiload.messagereceiver.common.domain.JobStatus.*;

import com.maiload.messagereceiver.orchestrator.repository.BulkJobRepository;
import com.maiload.messagereceiver.orchestrator.repository.BulkJobRepository.PendingJob;
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
import java.util.zip.GZIPInputStream;

@Slf4j
@Service
@RequiredArgsConstructor
public class BulkJobOrchestrationService {

    private final BulkJobRepository bulkJobRepository;
    private final MinioClient minioClient;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final JsonMapper jsonMapper;

    @Value("${orchestrator.minio.bucket}")
    private String bucket;

    @Value("${orchestrator.kafka.bulk-task-topic}")
    private String bulkTaskTopic;

    @Value("${orchestrator.job.chunk-size}")
    private int chunkSize;

    @Scheduled(fixedDelayString = "${orchestrator.job.poll-interval-ms}")
    public void pollAndProcess() {
        List<PendingJob> jobs = bulkJobRepository.findPendingJobs();

        for (PendingJob job : jobs) {
            try {
                processJob(job);
            } catch (Exception e) {
                log.error("Failed to process job: jobId={}", job.jobId(), e);
                bulkJobRepository.updateStatus(job.jobId(), FAILED);
                bulkJobRepository.incrementRetryCount(job.jobId());
            }
        }
    }

    private void processJob(PendingJob job) throws Exception {
        int resumeFrom = job.publishedChunks();
        int skipLines = resumeFrom * chunkSize;

        log.info("Processing bulk job: jobId={}, objectKey={}, resumeFrom={}",
                job.jobId(), job.objectKey(), resumeFrom);

        bulkJobRepository.updateStatus(job.jobId(), PROCESSING);

        int totalLines = 0;
        int chunkIndex = resumeFrom;
        int linesInCurrentChunk = 0;

        // MinIO에서 JSONL.gz 파일을 스트리밍으로 읽으며 chunkSize 단위로 Kafka 발행
        try (InputStream is = minioClient.getObject(
                GetObjectArgs.builder().bucket(bucket).object(job.objectKey()).build());
             BufferedReader reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(is)))) {

            while (reader.readLine() != null) {
                totalLines++;

                // 이미 발행된 라인 건너뛰기
                if (totalLines <= skipLines) continue;

                linesInCurrentChunk++;

                if (linesInCurrentChunk == chunkSize) {
                    int startOffset = chunkIndex * chunkSize;
                    publishChunk(job, chunkIndex, startOffset, startOffset + chunkSize);
                    chunkIndex++;
                    bulkJobRepository.updatePublishedChunks(job.jobId(), chunkIndex);
                    linesInCurrentChunk = 0;
                }
            }

            // 나머지 라인 처리
            if (linesInCurrentChunk > 0) {
                int startOffset = chunkIndex * chunkSize;
                publishChunk(job, chunkIndex, startOffset, startOffset + linesInCurrentChunk);
                chunkIndex++;
                bulkJobRepository.updatePublishedChunks(job.jobId(), chunkIndex);
            }
        }

        bulkJobRepository.updateTotalCount(job.jobId(), totalLines);
        bulkJobRepository.updateStatus(job.jobId(), PUBLISHED);

        log.info("Bulk job published: jobId={}, totalLines={}, chunks={}",
                job.jobId(), totalLines, chunkIndex);
    }

    private void publishChunk(PendingJob job, int chunkIndex, int startOffset, int endOffset) throws Exception {
        BulkSendTask task = new BulkSendTask(
                job.jobId(),
                chunkIndex,
                job.customerId(),
                job.templateId(),
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
            DatasetRef datasetRef
    ) {
        record DatasetRef(
                String objectKey,
                int startOffset,
                int endOffset
        ) {}
    }
}
