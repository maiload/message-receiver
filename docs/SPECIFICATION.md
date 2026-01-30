# 명세서

## 1. DB 테이블

모든 테이블은 `messaging` 스키마에 생성. Flyway 마이그레이션으로 관리 (`receiver/src/main/resources/db/migration/`).

### customers

| 컬럼 | 타입 | 설명 |
|------|------|------|
| id | BIGSERIAL PK | - |
| customer_id | VARCHAR(64) UNIQUE | 고객 식별자 |
| name | VARCHAR(128) | 고객명 |
| api_key_hash | VARCHAR(256) | API Key SHA-256 해시 |
| api_key_expires_at | TIMESTAMP | 만료 시각 (NULL = 무기한) |
| rate_limit_rps | INT (default 100) | 초당 요청 제한 |
| status | VARCHAR(16) (default 'ACTIVE') | 고객 상태 |
| created_at | TIMESTAMP | 생성 시각 |
| updated_at | TIMESTAMP | 수정 시각 |

### templates

| 컬럼 | 타입 | 설명 |
|------|------|------|
| id | BIGSERIAL PK | - |
| template_id | VARCHAR(64) UNIQUE | 템플릿 식별자 |
| customer_id | VARCHAR(64) FK | 소유 고객 |
| channel | VARCHAR(16) | SMS, LMS, MMS |
| name | VARCHAR(128) | 템플릿명 |
| content | TEXT | 내용 (`{{변수}}` 형태) |
| status | VARCHAR(16) (default 'ACTIVE') | 상태 |
| created_at | TIMESTAMP | 생성 시각 |
| updated_at | TIMESTAMP | 수정 시각 |

### cdr_records

| 컬럼 | 타입 | 설명 |
|------|------|------|
| id | BIGSERIAL PK | - |
| customer_id | VARCHAR(64) | 고객 ID |
| receipt_id | VARCHAR(64) | 영수증 ID |
| customer_message_id | VARCHAR(128) | 멱등 키 |
| channel | VARCHAR(16) | SMS, LMS, MMS |
| send_type | VARCHAR(16) (default 'REALTIME') | REALTIME, BULK |
| status | VARCHAR(32) | SENT, FAILED |
| provider_message_id | VARCHAR(128) | 외부 발송 ID |
| recipient_hash | VARCHAR(64) | 수신자 SHA-256 해시 |
| fail_code | VARCHAR(32) | 실패 코드 |
| fail_reason | VARCHAR(256) | 실패 사유 |
| accepted_at | TIMESTAMP | 수신 시각 |
| sent_at | TIMESTAMP | 발송 시각 |
| created_at | TIMESTAMP | 생성 시각 |

UNIQUE: `(customer_id, customer_message_id)`

> SKIPPED 상태 메시지는 cdr_records에 삽입하지 않음. bulk_jobs 카운트 집계에만 사용.

### bulk_jobs

| 컬럼 | 타입 | 설명 |
|------|------|------|
| id | BIGSERIAL PK | - |
| job_id | VARCHAR(64) UNIQUE | Job 식별자 |
| customer_id | VARCHAR(64) | 고객 ID |
| template_id | VARCHAR(64) | 템플릿 ID |
| object_key | VARCHAR(512) | MinIO 파일 경로 |
| status | VARCHAR(32) | Job 상태 |
| total_count | INT (default 0) | 전체 건수 |
| success_count | INT (default 0) | 성공 건수 |
| fail_count | INT (default 0) | 실패 건수 |
| skip_count | INT (default 0) | 스킵 건수 (중복) |
| published_chunks | INT (default 0) | 발행된 청크 수 |
| retry_count | INT (default 0) | 재시도 횟수 |
| scheduled_at | TIMESTAMP | 예약 발송 시각 |
| locked_by | VARCHAR(64) | 처리 중 잠금 인스턴스 |
| locked_until | TIMESTAMP | 잠금 만료 시각 |
| created_at | TIMESTAMP | 생성 시각 |
| started_at | TIMESTAMP | 시작 시각 |
| completed_at | TIMESTAMP | 완료 시각 |

### send_attempts

| 컬럼 | 타입 | 설명 |
|------|------|------|
| id | BIGSERIAL PK | - |
| customer_id | VARCHAR(64) | 고객 ID |
| customer_message_id | VARCHAR(128) | 멱등 키 |
| receipt_id | VARCHAR(64) | 영수증 ID |
| job_id | VARCHAR(64) | Job ID |
| status | VARCHAR(16) (default 'LOCKED') | 상태 |
| created_at | TIMESTAMP | 생성 시각 |

UNIQUE: `(customer_id, customer_message_id)` - Bulk 중복 발송 방지

---

## 2. Gradle 설정

### 멀티모듈 구성

```
settings.gradle: common, receiver, worker, cdr-writer, orchestrator
```

**루트 build.gradle**: Spring Boot 4.0.2, Java 21, Lombok 전체 적용

### 모듈별 주요 의존성

| 모듈 | 주요 의존성 |
|------|-----------|
| receiver | spring-grpc, spring-amqp, spring-data-redis, bucket4j, jooq, flyway, protobuf |
| worker | spring-amqp, spring-kafka, jooq, minio, resilience4j |
| cdr-writer | spring-kafka, jooq |
| orchestrator | spring-kafka, jooq, minio |

### jOOQ 설정

모든 DB 접근 모듈(receiver, cdr-writer, orchestrator)에 `forcedType` + `enumConverter` 적용:

| 대상 컬럼 | Enum |
|----------|------|
| `bulk_jobs.status` | `JobStatus` |
| `cdr_records.status` | `MessageStatus` |
| `cdr_records.send_type` | `SendType` |
| `cdr_records.channel` | `ChannelType` |

worker는 `send_attempts` 테이블만 사용 (forcedType 없음).

코드 생성: `generateSchemaSourceOnCompilation = false` → 수동 실행 (`./gradlew :모듈:generateJooq`)

---

## 3. 에러 코드

| 코드 | 분류 | 설명 | Retryable | gRPC Status |
|------|------|------|-----------|-------------|
| A001 | Auth | 인증 실패 | N | UNAUTHENTICATED |
| A002 | Auth | 권한 없음 (customerId 불일치) | N | PERMISSION_DENIED |
| A003 | Auth | API Key 만료 | N | UNAUTHENTICATED |
| V001 | Validation | 요청 형식 오류 | N | INVALID_ARGUMENT |
| V002 | Validation | 수신자 번호 오류 | N | INVALID_ARGUMENT |
| V003 | Validation | 유효하지 않은 템플릿 | N | INVALID_ARGUMENT |
| V004 | Validation | 템플릿 없음 | N | NOT_FOUND |
| V005 | Validation | 영수증 없음 | N | NOT_FOUND |
| V006 | Validation | Job 없음 | N | NOT_FOUND |
| P001 | Policy | TPS 제한 초과 | Y | RESOURCE_EXHAUSTED |
| P002 | Policy | 멱등 키 중복 | N | ALREADY_EXISTS |
| I001 | Infra | Redis 연결 실패 | Y | UNAVAILABLE |
| I002 | Infra | MQ 발행 실패 | Y | UNAVAILABLE |
| I003 | Infra | Kafka 발행 실패 | Y | UNAVAILABLE |
| I004 | Infra | DB 연결 실패 | Y | UNAVAILABLE |
| G001 | Gateway | 타임아웃 | Y | UNAVAILABLE |
| G002 | Gateway | 5xx 응답 | Y | UNAVAILABLE |
| G003 | Gateway | 4xx 영구 실패 | N | INTERNAL |

---

## 4. API 명세

### 인증

| 프로토콜 | 헤더 |
|---------|------|
| gRPC | `x-api-key` (metadata) |
| REST | `X-API-Key` (header) |

흐름: API Key → SHA-256 해시 → customers 테이블 조회 → customerId 일치 확인 → 만료 확인

### gRPC API (Realtime)

**Submit** (`RealtimeMessageService/Submit`)

Request:

| 필드 | 타입 | 필수 | 설명 |
|------|------|------|------|
| customer_id | string | O | 고객 ID |
| customer_message_id | string | O | 멱등 키 |
| message_type | enum | O | SMS, LMS, MMS |
| recipient | string | O | 수신자 전화번호 |
| template_id | string | O | 템플릿 ID |
| content | string | - | 직접 내용 (선택) |
| vars | map | - | 템플릿 변수 |
| ttl_seconds | int32 | - | 메시지 TTL |
| media_urls | repeated string | - | MMS 첨부 URL (최대 3개) |

Response:

| 필드 | 타입 | 설명 |
|------|------|------|
| receipt_id | string | 영수증 ID (UUID) |
| accepted_at | Timestamp | 수신 시각 |
| idempotency_hit | bool | 멱등 중복 여부 |

**GetReceiptStatus** (`RealtimeMessageService/GetReceiptStatus`)

Request: `customer_id`, `receipt_id`

Response:

| 필드 | 타입 | 설명 |
|------|------|------|
| receipt_id | string | 영수증 ID |
| customer_message_id | string | 멱등 키 |
| status | DeliveryStatus | SENT, FAILED |
| fail_code | string | 실패 코드 |
| fail_reason | string | 실패 사유 |
| accepted_at | Timestamp | 수신 시각 |
| sent_at | Timestamp | 발송 시각 |

### REST API (Bulk)

**Job 생성** `POST /bulk/jobs`

```json
{
  "templateId": "tpl-welcome-v1",
  "objectKey": "cust-001/2025/01/28/abc123.jsonl.gz",
  "scheduledAt": "2025-01-28T10:00:00"  // 생략 시 즉시 발송
}
```

Response (201):

```json
{
  "jobId": "job-uuid-12345",
  "createdAt": "2025-01-28T09:00:00"
}
```

**Job 상태 조회** `GET /bulk/jobs/{jobId}`

```json
{
  "jobId": "job-uuid-12345",
  "customerId": "cust-001",
  "status": "COMPLETED",
  "totalCount": 50000,
  "successCount": 48000,
  "failCount": 1000,
  "skipCount": 1000,
  "pendingCount": 0,
  "createdAt": "2025-01-28T09:00:00",
  "startedAt": "2025-01-28T10:00:00",
  "completedAt": "2025-01-28T10:05:00"
}
```

---

## 5. Kafka 토픽

### bulk.send.task

Partition Key: `jobId`

```json
{
  "jobId": "job-uuid-12345",
  "chunkIndex": 0,
  "customerId": "cust-001",
  "templateId": "tpl-welcome-v1",
  "channel": "SMS",
  "lines": [
    {"customerMessageId": "msg-001", "recipient": "01012345678", "vars": {"name": "홍길동"}}
  ]
}
```

### cdr.events

Partition Key: `customerId`

```json
{
  "eventId": "evt-uuid-11111",
  "eventType": "DELIVERY_RESULT",
  "occurredAt": "2025-01-28T09:00:05",
  "customerId": "cust-001",
  "receiptId": "receipt-uuid-67890",
  "customerMessageId": "msg-12345",
  "sendType": "REALTIME",
  "channel": "SMS",
  "status": "SENT",
  "recipientHash": "a1b2c3...",
  "providerMessageId": "ext-12345",
  "failCode": null,
  "failReason": null,
  "jobId": null
}
```

---

## 6. Redis 키 패턴

| 용도 | 키 | TTL |
|------|-----|-----|
| 멱등성 | `idem:{customerId}:{customerMessageId}` | 24시간 |
| Rate Limit | Bucket4j 내부 관리 | 설정값 |

---

## 7. MinIO 파일

| 항목 | 값 |
|------|-----|
| 버킷 | `message-receiver` |
| 형식 | JSON Lines + gzip (`.jsonl.gz`) |
| 경로 | `{customerId}/{yyyy}/{MM}/{dd}/{uuid}.jsonl.gz` |

```json
{"customerMessageId": "msg-001", "recipient": "01012345678", "vars": {"name": "홍길동"}}
{"customerMessageId": "msg-002", "recipient": "01087654321", "vars": {"name": "김철수"}}
```
