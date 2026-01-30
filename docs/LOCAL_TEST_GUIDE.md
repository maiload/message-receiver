# 로컬 테스트 가이드

## 사전 준비

| 도구 | 설치 | 용도 |
|------|------|------|
| Docker / Docker Compose | 필수 | 인프라 실행 |
| JDK 21 | 필수 | 애플리케이션 빌드/실행 |
| Postman | 필수 | API 테스트 (REST + gRPC) |

### Postman Collection 임포트

프로젝트에 Postman 파일이 포함되어 있습니다. Postman에서 **Import** → 아래 두 파일을 선택합니다.

| 파일 | 경로 |
|------|------|
| Collection | `docs/postman/message-receiver.postman_collection.json` |
| Environment | `docs/postman/message-receiver-local.postman_environment.json` |

임포트 후 우측 상단에서 환경을 **message-receiver-local**로 선택합니다.

> **gRPC 요청**은 Postman Collection에 포함할 수 없어 직접 생성해야 합니다. Step 5에서 안내합니다.

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
| postgres | 5432 | healthcheck |
| redis | 6379 | healthcheck |
| rabbitmq | 5672 / 15672 | http://localhost:15672 (maiload / maiload) |
| kafka | 9094 | Kafka UI http://localhost:8080 |
| minio | 9000 / 9001 | http://localhost:9001 (maiload / maiload123) |

---

## Step 2: Kafka 토픽 생성

Kafka는 `auto.create.topics.enable=false`이므로 수동 생성이 필요합니다.

```bash
docker exec message-receiver-kafka bash /opt/kafka/init/create-topics.sh
```

확인:

```bash
docker exec message-receiver-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list
```

---

## Step 3: MinIO 버킷 생성

버킷 이름: `message-receiver`

**방법 A: MinIO Console (GUI)**

1. http://localhost:9001 접속
2. maiload / maiload123 로그인
3. Buckets > Create Bucket > `message-receiver` 입력 > Create

**방법 B: CLI**

```bash
docker exec message-receiver-minio mc alias set local http://localhost:9000 maiload maiload123
docker exec message-receiver-minio mc mb local/message-receiver
```

---

## Step 4: 애플리케이션 기동

**receiver를 가장 먼저 기동**합니다. Flyway 마이그레이션이 DB 스키마 생성과 시드 데이터 삽입을 자동으로 처리합니다.

> **시드 데이터**: 테스트 고객 `cust-001` (API Key: `test-api-key`)과 SMS 템플릿 2개(`tpl-hello`, `tpl-simple`)가 자동 삽입됩니다.

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

기동 확인: Postman에서 **Health Checks** 폴더의 요청을 실행합니다.

| 모듈 | 포트 | 비고 |
|------|------|------|
| receiver | 8081 (REST) / 9090 (gRPC) | |
| worker | 8082 | |
| cdr-writer | 8083 | |
| orchestrator | 8084 | |

---

## Step 5: Realtime 메시지 테스트 (gRPC)

gRPC 요청은 Postman Collection에 포함할 수 없으므로 직접 생성합니다.

### 5-1. Postman gRPC 요청 생성

1. 좌측 상단 **New** > **gRPC** 선택
2. **Enter URL**: `localhost:9090`
3. 서버에 연결되면 **Select a method** 드롭다운에 서비스 목록이 자동으로 표시됨 (Server Reflection)
4. **Metadata** 탭에서 `x-api-key` = `test-api-key` 추가
5. **Message** 탭에서 JSON 입력 후 **Invoke**

### 5-2. Submit (메시지 발송)

Method: `RealtimeMessageService/Submit`

```json
{
  "customer_id": "cust-001",
  "customer_message_id": "msg-test-001",
  "message_type": "SMS",
  "recipient": "01012345678",
  "template_id": "tpl-hello",
  "vars": {"name": "홍길동", "message": "테스트입니다"}
}
```

> **참고**: Postman gRPC는 proto 원본 필드명(snake_case)을 사용합니다.

성공 응답 예시:

```json
{
  "receipt_id": "550e8400-e29b-41d4-a716-446655440000",
  "accepted_at": "2025-01-29T12:00:00Z",
  "idempotency_hit": false
}
```

### 5-3. 멱등성 테스트

동일한 `customer_message_id`(`msg-test-001`)로 Submit을 다시 호출하면 `idempotency_hit: true`가 반환됩니다.

### 5-4. GetReceiptStatus (상태 조회)

Method: `RealtimeMessageService/GetReceiptStatus`

```json
{
  "customer_id": "cust-001",
  "receipt_id": "<Submit 응답의 receipt_id>"
}
```

### 5-5. 에러 케이스

| 테스트 | 변경 사항 | 기대 응답 |
|--------|----------|----------|
| 인증 실패 | Metadata `x-api-key: wrong-key` | `UNAUTHENTICATED` |
| 고객 ID 불일치 | `customer_id: cust-999` | `PERMISSION_DENIED` |
| 템플릿 없음 | `template_id: tpl-nonexistent` | `NOT_FOUND` |

---

## Step 6: Bulk 메시지 테스트 (REST)

Postman의 **Bulk** 폴더에 번호 순서대로 요청이 준비되어 있습니다.

### 6-1. 테스트 파일 준비

터미널에서 테스트 JSONL 파일을 생성합니다:

프로젝트 루트에서 실행합니다:

```bash
cat > bulk-test.jsonl << 'EOF'
{"customerMessageId": "bulk-001", "recipient": "01012345678", "vars": {"name": "홍길동", "message": "대량 발송 1"}}
{"customerMessageId": "bulk-002", "recipient": "01087654321", "vars": {"name": "김철수", "message": "대량 발송 2"}}
{"customerMessageId": "bulk-003", "recipient": "01011112222", "vars": {"name": "이영희", "message": "대량 발송 3"}}
EOF

gzip -k bulk-test.jsonl
```

### 6-2. MinIO 업로드

Postman에서 **Bulk > 1. Upload Test File to MinIO** 실행:
- Body > binary에서 `bulk-test.jsonl.gz` 파일 선택
- AWS Signature 인증이 자동 적용됨 (Environment의 MinIO 키 사용)

### 6-3. 즉시 발송 Job 생성

**Bulk > 2. Create Bulk Job** 실행. 즉시 발송 Job이 생성되고, 응답의 `jobId`가 자동으로 Collection 변수에 저장됩니다.

### 6-4. 예약 발송 Job 생성

**Bulk > 3. Create Scheduled Bulk Job** 실행. Pre-request Script가 현재 시각 + 3분을 `scheduledAt`에 자동 설정합니다.

생성 후 상태를 조회하면 `PENDING` 상태가 유지되고, 3분 후 Orchestrator 폴링 시점에 처리가 시작됩니다.

### 6-5. Job 상태 조회

**Bulk > 4. Get Bulk Job Status** 실행. `{{jobId}}`가 자동으로 채워집니다.

Orchestrator가 5초 간격으로 폴링하므로, 잠시 후 상태가 `PENDING` → `PROCESSING` → `PUBLISHED` → `DELIVERED`로 변경됩니다.

---

## Step 7: 결과 확인

### RabbitMQ 관리 UI

http://localhost:15672 > Queues 탭

| 큐 | 확인 사항 |
|----|----------|
| `message.realtime.queue` | 메시지가 Worker에 의해 소비되는지 |
| `message.realtime.retry.*` | 재시도 메시지 존재 여부 |
| `message.realtime.dlq` | DLQ 적재 여부 (정상이면 0) |

### Kafka UI

http://localhost:8080 > Topics

| 토픽 | 확인 사항 |
|------|----------|
| `cdr.events` | CDR 이벤트 메시지 발행 여부 |
| `bulk.send.task` | Bulk 청크 메시지 발행 여부 |

### DB 확인

```bash
# CDR 레코드
docker exec message-receiver-postgres psql -U maiload -d message_receiver \
  -c "SELECT receipt_id, customer_message_id, channel, send_type, status
      FROM messaging.cdr_records ORDER BY created_at DESC LIMIT 10;"

# Bulk Job 상태
docker exec message-receiver-postgres psql -U maiload -d message_receiver \
  -c "SELECT job_id, status, total_count, published_chunks
      FROM messaging.bulk_jobs ORDER BY created_at DESC LIMIT 5;"
```

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

시드 데이터 삽입 여부 확인:

```bash
docker exec message-receiver-postgres psql -U maiload -d message_receiver \
  -c "SELECT customer_id, api_key_hash, status FROM messaging.customers;"
```

데이터가 없으면 Postgres를 초기화하고 receiver를 재기동하세요:

```bash
cd docker
docker-compose stop postgres
rm -rf data/postgres/
docker-compose up -d postgres
# Postgres healthy 확인 후 receiver 재기동
```

### Kafka 토픽 관련 오류

```bash
docker exec message-receiver-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list
```

`cdr.events`, `bulk.send.task`가 목록에 있어야 합니다.

### CDR 레코드가 DB에 없음

1. Worker 로그에서 Gateway 호출 성공/실패 확인
2. Kafka UI에서 `cdr.events` 토픽에 메시지 있는지 확인
3. CDR Writer 로그에서 배치 처리 로그 확인

### MinIO 파일 접근 오류

```bash
docker exec message-receiver-minio mc ls local/message-receiver/
```
