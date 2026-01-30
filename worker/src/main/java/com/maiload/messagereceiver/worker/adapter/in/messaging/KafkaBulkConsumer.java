package com.maiload.messagereceiver.worker.adapter.in.messaging;

import com.maiload.messagereceiver.common.domain.ChannelType;
import com.maiload.messagereceiver.common.domain.SendType;
import com.maiload.messagereceiver.common.util.IdGenerator;
import com.maiload.messagereceiver.common.util.PhoneNumberUtils;
import com.maiload.messagereceiver.worker.adapter.out.persistence.SendAttemptRepository;
import com.maiload.messagereceiver.worker.application.port.in.MessageProcessPort;
import io.minio.GetObjectArgs;
import io.minio.MinioClient;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaBulkConsumer {

    private final MessageProcessPort messageProcessPort;
    private final SendAttemptRepository sendAttemptRepository;
    private final MinioClient minioClient;
    private final JsonMapper jsonMapper;

    @Value("${worker.minio.bucket}")
    private String bucket;

    private static final Pattern PLACEHOLDER_PATTERN = Pattern.compile("\\{\\{(\\w+)}}");

    @KafkaListener(topics = "${worker.kafka.bulk-task-topic}", groupId = "${spring.kafka.consumer.group-id}")
    public void onMessage(String message) {
        BulkSendTask task = jsonMapper.readValue(message, BulkSendTask.class);

        log.info("Received bulk task: jobId={}, chunkIndex={}, offset={}-{}",
                task.jobId(), task.chunkIndex(),
                task.datasetRef().startOffset(), task.datasetRef().endOffset());

        int[] result = processChunk(task);

        log.info("Bulk task completed: jobId={}, chunkIndex={}, sent={}, skipped={}, failed={}",
                task.jobId(), task.chunkIndex(), result[0], result[1], result[2]);
    }

    private int[] processChunk(BulkSendTask task) {
        int sent = 0;
        int skipped = 0;
        int failed = 0;

        try (InputStream is = minioClient.getObject(
                GetObjectArgs.builder().bucket(bucket).object(task.datasetRef().objectKey()).build());
             BufferedReader reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(is)))) {

            int lineNumber = 0;
            int startOffset = task.datasetRef().startOffset();
            int endOffset = task.datasetRef().endOffset();
            String line;

            while ((line = reader.readLine()) != null) {
                if (lineNumber < startOffset) {
                    lineNumber++;
                    continue;
                }
                if (lineNumber >= endOffset) break;
                lineNumber++;

                try {
                    BulkLineDto lineDto = jsonMapper.readValue(line, BulkLineDto.class);
                    String receiptId = IdGenerator.uuid();

                    // 중복 발송 방지: DB 락 획득 시도
                    boolean acquired = sendAttemptRepository.tryLock(
                            task.customerId(), lineDto.customerMessageId(), receiptId, task.jobId());

                    if (!acquired) {
                        skipped++;
                        log.debug("Skipping duplicate: jobId={}, customerMessageId={}",
                                task.jobId(), lineDto.customerMessageId());
                        continue;
                    }

                    messageProcessPort.process(toProcess(task, lineDto, receiptId));
                    sendAttemptRepository.updateStatus(
                            task.customerId(), lineDto.customerMessageId(), "SENT");
                    sent++;
                } catch (Exception e) {
                    failed++;
                    log.warn("Failed to process bulk line: jobId={}, line={}, error={}",
                            task.jobId(), lineNumber, e.getMessage());
                }
            }
        } catch (Exception e) {
            log.error("Failed to read bulk file: jobId={}, objectKey={}",
                    task.jobId(), task.datasetRef().objectKey(), e);
            throw new RuntimeException("Failed to process bulk task", e);
        }

        return new int[]{sent, skipped, failed};
    }

    private MessageProcessPort.Process toProcess(BulkSendTask task, BulkLineDto lineDto, String receiptId) {
        return new MessageProcessPort.Process(
                receiptId,
                task.customerId(),
                lineDto.customerMessageId(),
                SendType.BULK,
                ChannelType.valueOf(task.channel()),
                PhoneNumberUtils.normalize(lineDto.recipient()),
                task.templateId(),
                renderTemplate(task.contentTemplate(), lineDto.vars()),
                lineDto.vars(),
                null,
                List.of(),
                null,
                task.jobId()
        );
    }

    private String renderTemplate(String template, Map<String, String> vars) {
        if (vars == null || vars.isEmpty()) return template;

        Matcher matcher = PLACEHOLDER_PATTERN.matcher(template);
        StringBuilder sb = new StringBuilder();
        while (matcher.find()) {
            String key = matcher.group(1);
            String value = vars.getOrDefault(key, matcher.group(0));
            matcher.appendReplacement(sb, Matcher.quoteReplacement(value));
        }
        matcher.appendTail(sb);
        return sb.toString();
    }

    private record BulkSendTask(
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

    private record BulkLineDto(
            String customerMessageId,
            String recipient,
            Map<String, String> vars
    ) {}
}
