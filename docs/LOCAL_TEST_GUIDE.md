# 로컬 테스트 가이드

## 사전 준비

| 도구 | 설치 | 용도 |
|------|------|------|
| Docker / Docker Compose | 필수 | 인프라 실행 |
| JDK 21 | 필수 | 애플리케이션 빌드/실행 |
| grpcurl | `brew install grpcurl` | gRPC 테스트 |
| psql | `brew install libpq` | DB 시드 데이터 |

---

## Step 1: 인프라 기동

```bash
cd docker
docker-compose up -d
```

모든 컨테이너가 healthy 상태인지 확인:

```bash
docker-compose ps
```

| 서비스 | 포트 | 확인 방법 |
|--------|------|----------|
| postgres | 5432 | `psql -h localhost -U maiload -d message_receiver -c '\l'` |
| redis | 6379 | `docker exec message-receiver-redis redis-cli ping` |
| rabbitmq | 5672 / 15672 | http://localhost:15672 (maiload / maiload) |
| kafka | 9094 | Kafka UI http://localhost:8080 |
| minio | 9000 / 9001 | http://localhost:9001 (maiload / maiload123) |

---

## Step 2: Kafka 토픽 생성

Kafka는 `auto.create.topics.enable=false`이므로 수동 생성이 필요합니다.

```bash
# 초기화 스크립트 실행
docker exec message-receiver-kafka bash /opt/kafka/init/create-topics.sh
```

또는 개별 생성:

```bash
docker exec message-receiver-kafka /opt/kafka/bin/kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic cdr.events --partitions 6 --if-not-exists

docker exec message-receiver-kafka /opt/kafka/bin/kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic bulk.send.task --partitions 6 --if-not-exists
```

확인:

```bash
docker exec message-receiver-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list
```

---

## Step 3: MinIO 버킷 생성

버킷 자동 생성이 없으므로 수동으로 만들어야 합니다. 버킷 이름: `message-receiver`

**방법 A: MinIO Console (GUI)**

1. http://localhost:9001 접속
2. maiload / maiload123 로그인
3. Buckets → Create Bucket → `message-receiver` 입력 → Create

**방법 B: CLI**

```bash
docker exec message-receiver-minio mc alias set local http://localhost:9000 maiload maiload123
docker exec message-receiver-minio mc mb local/message-receiver
```

---

## Step 4: 애플리케이션 기동

**receiver를 가장 먼저 기동**합니다. Flyway 마이그레이션이 receiver에서 실행되므로 DB 스키마가 먼저 준비되어야 합니다.

터미널을 4개 열고 각각 실행:

```bash
# 터미널 1 - receiver (가장 먼저)
./gradlew :receiver:bootRun

# receiver 기동 완료 후 나머지 (순서 무관)

# 터미널 2 - worker
./gradlew :worker:bootRun

# 터미널 3 - cdr-writer
./gradlew :cdr-writer:bootRun

# 터미널 4 - orchestrator
./gradlew :orchestrator:bootRun
```

| 모듈 | HTTP 포트 | gRPC 포트 | 기동 확인 |
|------|-----------|-----------|----------|
| receiver | 8081 | 9090 | `curl localhost:8081/actuator/health` |
| worker | 8082 | - | `curl localhost:8082/actuator/health` |
| cdr-writer | 8083 | - | `curl localhost:8083/actuator/health` |
| orchestrator | 8084 | - | `curl localhost:8084/actuator/health` |

---

## Step 5: 시드 데이터 삽입

인증과 템플릿 검증을 위해 테스트 데이터가 필요합니다.

API Key 인증은 **SHA-256 해시**로 비교합니다. `test-api-key`의 해시를 DB에 넣겠습니다.

```bash
psql -h localhost -U maiload -d message_receiver << 'SQL'

-- pgcrypto 확장 (SHA-256 해시 생성용)
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- 테스트 고객
-- API Key: test-api-key → SHA-256 해시로 저장
INSERT INTO messaging.customers (customer_id, name, api_key_hash, rate_limit_rps, rate_limit_burst)
VALUES (
    'cust-001',
    'Test Customer',
    encode(digest('test-api-key', 'sha256'), 'hex'),
    100,
    200
) ON CONFLICT (customer_id) DO NOTHING;

-- SMS 템플릿
INSERT INTO messaging.templates (template_id, customer_id, channel, name, content)
VALUES (
    'tpl-hello',
    'cust-001',
    'SMS',
    '인사 템플릿',
    '안녕하세요 {{name}}님, {{message}}'
) ON CONFLICT (template_id) DO NOTHING;

-- 변수 없는 단순 템플릿
INSERT INTO messaging.templates (template_id, customer_id, channel, name, content)
VALUES (
    'tpl-simple',
    'cust-001',
    'SMS',
    '단순 알림',
    '서비스 점검 안내입니다.'
) ON CONFLICT (template_id) DO NOTHING;

SQL
```

확인:

```bash
psql -h localhost -U maiload -d message_receiver \
  -c "SELECT customer_id, name, status FROM messaging.customers;"
```

---

## Step 6: Realtime 메시지 테스트 (gRPC)

gRPC는 HTTP/2 프로토콜이므로 `grpcurl`을 사용합니다. 로컬에서는 TLS 없이 `-plaintext` 옵션을 사용합니다.

> **참고**: `@GlobalServerInterceptor`로 인증이 모든 gRPC 호출에 적용됩니다.
> `grpcurl list` 등 Reflection 호출에도 `-H 'x-api-key: ...'`가 필요합니다.

### 6-1. 서비스 목록 확인

```bash
grpcurl -plaintext \
  -H 'x-api-key: test-api-key' \
  localhost:9090 list
```

proto 파일로 직접 확인하려면:

```bash
grpcurl -plaintext \
  -import-path receiver/src/main/proto \
  -proto realtime_message.proto \
  localhost:9090 describe com.maiload.messagereceiver.grpc.RealtimeMessageService
```

### 6-2. Submit (메시지 발송 요청)

```bash
grpcurl -plaintext \
  -H 'x-api-key: test-api-key' \
  -d '{
    "customerId": "cust-001",
    "customerMessageId": "msg-test-001",
    "messageType": "SMS",
    "recipient": "01012345678",
    "templateId": "tpl-hello",
    "vars": {"name": "홍길동", "message": "테스트입니다"}
  }' \
  localhost:9090 com.maiload.messagereceiver.grpc.RealtimeMessageService/Submit
```

성공 응답 예시:

```json
{
  "receiptId": "550e8400-e29b-41d4-a716-446655440000",
  "acceptedAt": "2025-01-29T12:00:00Z",
  "idempotencyHit": false
}
```

### 6-3. 멱등성 테스트

같은 `customerMessageId`로 다시 호출하면 `idempotencyHit: true`가 반환됩니다:

```bash
# 동일한 msg-test-001로 재호출
grpcurl -plaintext \
  -H 'x-api-key: test-api-key' \
  -d '{
    "customerId": "cust-001",
    "customerMessageId": "msg-test-001",
    "messageType": "SMS",
    "recipient": "01012345678",
    "templateId": "tpl-hello",
    "vars": {"name": "홍길동", "message": "테스트입니다"}
  }' \
  localhost:9090 com.maiload.messagereceiver.grpc.RealtimeMessageService/Submit
```

### 6-4. GetReceiptStatus (상태 조회)

Submit 응답의 `receiptId`를 사용합니다:

```bash
grpcurl -plaintext \
  -H 'x-api-key: test-api-key' \
  -d '{
    "customerId": "cust-001",
    "receiptId": "<Submit 응답의 receiptId>"
  }' \
  localhost:9090 com.maiload.messagereceiver.grpc.RealtimeMessageService/GetReceiptStatus
```

### 6-5. 에러 케이스 테스트

```bash
# 인증 실패 (잘못된 API Key)
grpcurl -plaintext \
  -H 'x-api-key: wrong-key' \
  -d '{"customerId":"cust-001","customerMessageId":"msg-err-001","messageType":"SMS","recipient":"01012345678","templateId":"tpl-hello","vars":{"name":"홍길동","message":"test"}}' \
  localhost:9090 com.maiload.messagereceiver.grpc.RealtimeMessageService/Submit
# → UNAUTHENTICATED

# 고객 ID 불일치
grpcurl -plaintext \
  -H 'x-api-key: test-api-key' \
  -d '{"customerId":"cust-999","customerMessageId":"msg-err-002","messageType":"SMS","recipient":"01012345678","templateId":"tpl-hello","vars":{"name":"홍길동","message":"test"}}' \
  localhost:9090 com.maiload.messagereceiver.grpc.RealtimeMessageService/Submit
# → PERMISSION_DENIED

# 존재하지 않는 템플릿
grpcurl -plaintext \
  -H 'x-api-key: test-api-key' \
  -d '{"customerId":"cust-001","customerMessageId":"msg-err-003","messageType":"SMS","recipient":"01012345678","templateId":"tpl-nonexistent","vars":{}}' \
  localhost:9090 com.maiload.messagereceiver.grpc.RealtimeMessageService/Submit
# → NOT_FOUND
```

---

## Step 7: Bulk 메시지 테스트 (REST)

### 7-1. 테스트 파일 생성 및 MinIO 업로드

```bash
# JSONL 파일 생성
cat > /tmp/bulk-test.jsonl << 'EOF'
{"customerMessageId": "bulk-001", "recipient": "01012345678", "vars": {"name": "홍길동", "message": "대량 발송 1"}}
{"customerMessageId": "bulk-002", "recipient": "01087654321", "vars": {"name": "김철수", "message": "대량 발송 2"}}
{"customerMessageId": "bulk-003", "recipient": "01011112222", "vars": {"name": "이영희", "message": "대량 발송 3"}}
EOF

# gzip 압축
gzip -k /tmp/bulk-test.jsonl

# MinIO에 업로드
docker exec message-receiver-minio mc alias set local http://localhost:9000 maiload maiload123
docker cp /tmp/bulk-test.jsonl.gz message-receiver-minio:/tmp/
docker exec message-receiver-minio mc cp /tmp/bulk-test.jsonl.gz local/message-receiver/cust-001/2025/01/29/bulk-test.jsonl.gz
```

### 7-2. Job 생성

```bash
curl -s -X POST http://localhost:8081/bulk/jobs \
  -H 'Content-Type: application/json' \
  -H 'X-API-Key: test-api-key' \
  -d '{
    "customerId": "cust-001",
    "templateId": "tpl-hello",
    "objectKey": "cust-001/2025/01/29/bulk-test.jsonl.gz"
  }' | jq .
```

응답 예시:

```json
{
  "jobId": "job-uuid-12345",
  "createdAt": "2025-01-29T12:00:00"
}
```

### 7-3. Job 상태 조회

```bash
curl -s http://localhost:8081/bulk/jobs/<jobId> \
  -H 'X-API-Key: test-api-key' | jq .
```

Orchestrator가 5초 간격으로 폴링하므로, 잠시 후 상태가 `PENDING` → `PROCESSING` → `PUBLISHED`로 변경됩니다.

---

## Step 8: 결과 확인

### DB - CDR 레코드

Realtime 메시지가 Worker → Gateway → CDR Writer를 거쳐 DB에 기록되었는지 확인:

```bash
psql -h localhost -U maiload -d message_receiver \
  -c "SELECT receipt_id, customer_message_id, channel, send_type, status, accepted_at
      FROM messaging.cdr_records
      ORDER BY created_at DESC
      LIMIT 10;"
```

### DB - Bulk Job 상태

```bash
psql -h localhost -U maiload -d message_receiver \
  -c "SELECT job_id, status, total_count, published_chunks, retry_count
      FROM messaging.bulk_jobs
      ORDER BY created_at DESC
      LIMIT 5;"
```

### RabbitMQ 관리 UI

http://localhost:15672 → Queues 탭

| 큐 | 확인 사항 |
|----|----------|
| `message.realtime.queue` | 메시지가 Worker에 의해 소비되는지 |
| `message.realtime.retry.*` | 재시도 메시지 존재 여부 |
| `message.realtime.dlq` | DLQ 적재 여부 (정상이면 0) |

### Kafka UI

http://localhost:8080 → Topics

| 토픽 | 확인 사항 |
|------|----------|
| `cdr.events` | CDR 이벤트 메시지 발행 여부 |
| `bulk.send.task` | Bulk 청크 메시지 발행 여부 |

---

## 정리 (종료)

```bash
# 애플리케이션: 각 터미널에서 Ctrl+C

# 인프라 종료 (데이터 유지)
cd docker
docker-compose down

# 인프라 종료 + 데이터 삭제
docker-compose down -v
rm -rf data/
```

---

## 문제 해결

### receiver 기동 실패: Flyway 오류

PostgreSQL이 아직 준비되지 않았을 수 있습니다. Docker healthcheck 확인 후 재시도:

```bash
docker-compose ps  # postgres가 healthy인지 확인
```

### gRPC 호출 시 UNAUTHENTICATED

시드 데이터가 올바르게 삽입되었는지 확인:

```bash
psql -h localhost -U maiload -d message_receiver \
  -c "SELECT customer_id, api_key_hash, status FROM messaging.customers;"
```

API Key `test-api-key`의 SHA-256 해시와 DB 값이 일치하는지 확인:

```bash
echo -n "test-api-key" | shasum -a 256 | awk '{print $1}'
```

### Kafka 토픽 관련 오류

토픽이 생성되었는지 확인:

```bash
docker exec message-receiver-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list
```

`cdr.events`, `bulk.send.task`가 목록에 있어야 합니다.

### CDR 레코드가 DB에 없음

Worker → CDR Writer 흐름을 확인:

1. Worker 로그에서 Gateway 호출 성공/실패 확인
2. Kafka UI에서 `cdr.events` 토픽에 메시지 있는지 확인
3. CDR Writer 로그에서 배치 처리 로그 확인

### MinIO 파일 접근 오류

버킷 존재 여부 및 파일 확인:

```bash
docker exec message-receiver-minio mc ls local/message-receiver/
```
