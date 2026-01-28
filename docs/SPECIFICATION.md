# Message Receiver 명세서

## 1. 프로젝트 개요

**Message Receiver**는 대규모 메시지 수신 및 발송을 처리하는 고처리량 메시징 시스템입니다.

### 1.1 핵심 가치

| 우선순위 | 설명 |
|---------|------|
| 1순위 | **신뢰성** - 원시 처리량보다 안정적인 메시지 전달 |
| 2순위 | **관심사 분리** - 명확한 도메인 경계 |
| 3순위 | **운영 안전성** - 재시도, DLQ, 백프레셔 지원 |
| 4순위 | **장기 유지보수성** - 과도한 설계 지양 |

### 1.2 v1 범위

#### 포함 (In Scope)

| 기능 | 상세 |
|------|------|
| Realtime 메시지 | gRPC Submit, SMS/LMS/MMS |
| Bulk 메시지 | REST Job 생성, JSONL+gzip |
| 인증 | API Key + customerId 검증 |
| 멱등성 | Redis + DB Unique |
| Rate Limit | Bucket4j (고객별) |
| 재시도/DLQ | RabbitMQ DLX + TTL |
| CDR | Kafka + 배치 삽입 |
| 템플릿 | DB 기반, placeholder 검증 |

#### 제외/보류 (Out of Scope)

| 기능 | 보류 이유 | 예정 |
|------|----------|------|
| Outbox 패턴 | MVP 복잡도, Kafka 자체 재시도로 충분 | v2 |
| mTLS | 운영 난이도 높음 | v2 |
| 템플릿 버저닝/승인 워크플로우 | 복잡도 | v2 |
| MMS 바이너리 업로드 | 범위 확대 | v2 |
| 웹훅 서명 (HMAC) | 단순화 우선 | v2 |
| Schema Registry (Avro) | JSON으로 시작 | v2 |

---

## 2. 시스템 아키텍처

### 2.1 메시지 흐름

#### 실시간(Realtime) 메시지

```
Client ──gRPC──▶ Receiver
                    ├─ Redis (멱등성 검사)
                    ├─ Redis (Rate Limit - Bucket4j)
                    └─ RabbitMQ (실시간 큐)
                           └─ Worker ──▶ Gateway (외부 발송)
                                             └─ Kafka (cdr.events)
                                                    └─ CDR Writer ──▶ DB
```

#### 대량(Bulk) 메시지

```
Client ──REST──▶ Receiver (Job 제출)
                    └─ MinIO (파일 업로드)
                           └─ Orchestrator ──▶ Kafka (bulk.send.task)
                                                   └─ Worker ──▶ Gateway
                                                                     └─ Kafka (cdr.events)
```

### 2.2 배포 단위

| 모듈 | 역할 | 스케일링 |
|------|------|----------|
| `receiver` | gRPC/REST API 진입점 | Horizontal (로드밸런서) |
| `worker` | MQ/Kafka Consumer + Gateway 발송 | Horizontal (Consumer Group) |
| `cdr-writer` | CDR 이벤트 소비 + DB 적재 | Horizontal (Kafka Partition 수 이내) |
| `orchestrator` | Bulk Job/Chunk 관리 | 단일 또는 소수 인스턴스 |

---

## 3. API 명세

### 3.1 인증

#### API Key 방식

| 프로토콜 | 헤더 |
|---------|------|
| gRPC | `x-api-key` (metadata) |
| REST | `X-API-Key` (header) |

#### 인증 흐름

```
1. 클라이언트 → API Key 전송
2. 서버 → apiKey로 customer 조회
3. 서버 → 요청 body의 customerId와 조회된 customerId 일치 확인
4. 불일치 시 → UNAUTHENTICATED (gRPC) / 401 (REST)
5. 만료 검사 → api_key_expires_at 확인
```

### 3.2 gRPC API (Realtime)

#### 서비스 정의

```protobuf
service RealtimeMessageService {
  rpc Submit(SubmitRequest) returns (SubmitResponse);
  rpc GetReceiptStatus(GetReceiptStatusRequest) returns (GetReceiptStatusResponse);
}
```

#### Submit Request

```protobuf
message SubmitRequest {
  string customer_id = 1;                    // 고객 ID (필수)
  string customer_message_id = 2;            // 멱등 키 (필수)
  MessageType message_type = 3;              // SMS/LMS/MMS (필수)
  string recipient = 4;                      // 수신자 전화번호 (필수)
  string template_id = 5;                    // 템플릿 ID (필수, v1)
  string content = 6;                        // 내용 - MVP에서만 허용 (선택)
  map<string, string> vars = 7;              // 템플릿 변수 (선택)
  int32 ttl_seconds = 8;                     // 메시지 TTL (선택)
  repeated string media_urls = 9;            // MMS 첨부 URL (최대 3개)
}

enum MessageType {
  MESSAGE_TYPE_UNSPECIFIED = 0;
  SMS = 1;
  LMS = 2;
  MMS = 3;
}
```

#### Submit Response

```protobuf
message SubmitResponse {
  string receipt_id = 1;                     // 영수증 ID (UUID)
  google.protobuf.Timestamp accepted_at = 2; // 수신 시각
  bool idempotency_hit = 3;                  // 멱등 중복 여부
}
```

#### gRPC 에러 코드

| gRPC Status | 조건 |
|-------------|------|
| `INVALID_ARGUMENT` | 필수 필드 누락, 포맷 오류 |
| `UNAUTHENTICATED` | API Key 누락/유효하지 않음 |
| `PERMISSION_DENIED` | customerId 불일치 |
| `RESOURCE_EXHAUSTED` | Rate limit 초과 |
| `ALREADY_EXISTS` | 멱등 키 중복 |
| `UNAVAILABLE` | 인프라 일시 장애 |

### 3.3 REST API (Bulk)

#### Job 생성

```
POST /bulk/jobs
Content-Type: application/json
X-API-Key: {api-key}
```

```json
{
  "customerId": "cust-001",
  "templateId": "tpl-welcome-v1",
  "objectKey": "cust-001/2025/01/28/abc123.jsonl.gz",
  "scheduledAt": "2025-01-28T10:00:00Z",
  "callbackUrl": "https://client.example.com/webhook/bulk"
}
```

#### Job 상태 조회

```
GET /bulk/jobs/{jobId}
```

```json
{
  "jobId": "job-uuid-12345",
  "customerId": "cust-001",
  "status": "PROCESSING",
  "totalCount": 50000,
  "successCount": 45000,
  "failCount": 1000,
  "pendingCount": 4000,
  "createdAt": "2025-01-28T09:00:00Z",
  "startedAt": "2025-01-28T10:00:00Z"
}
```

---

## 4. 데이터 스키마

### 4.1 PostgreSQL

#### customers 테이블

```sql
CREATE TABLE customers (
    id                  BIGSERIAL PRIMARY KEY,
    customer_id         VARCHAR(64) NOT NULL UNIQUE,
    name                VARCHAR(128) NOT NULL,
    api_key_hash        VARCHAR(256) NOT NULL,
    api_key_expires_at  TIMESTAMP,              -- NULL = 무기한

    -- Rate Limit
    rate_limit_rps      INT NOT NULL DEFAULT 100,
    rate_limit_burst    INT NOT NULL DEFAULT 200,

    -- 가격 정책
    sms_unit_price      BIGINT NOT NULL DEFAULT 20,
    lms_unit_price      BIGINT NOT NULL DEFAULT 50,
    mms_unit_price      BIGINT NOT NULL DEFAULT 100,

    status              VARCHAR(16) NOT NULL DEFAULT 'ACTIVE',
    created_at          TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMP NOT NULL DEFAULT NOW()
);
```

#### templates 테이블

```sql
CREATE TABLE templates (
    id                  BIGSERIAL PRIMARY KEY,
    template_id         VARCHAR(64) NOT NULL UNIQUE,
    customer_id         VARCHAR(64) NOT NULL REFERENCES customers(customer_id),
    channel             VARCHAR(16) NOT NULL,  -- SMS, LMS, MMS
    name                VARCHAR(128) NOT NULL,
    content             TEXT NOT NULL,         -- {{name}} 형태

    status              VARCHAR(16) NOT NULL DEFAULT 'ACTIVE',
    created_at          TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMP NOT NULL DEFAULT NOW()
);
```

#### cdr_records 테이블

```sql
CREATE TABLE cdr_records (
    id                    BIGSERIAL PRIMARY KEY,
    customer_id           VARCHAR(64) NOT NULL,
    receipt_id            VARCHAR(64) NOT NULL,
    customer_message_id   VARCHAR(128) NOT NULL,
    channel               VARCHAR(16) NOT NULL,
    status                VARCHAR(32) NOT NULL,
    provider_message_id   VARCHAR(128),
    recipient_hash        VARCHAR(64) NOT NULL,  -- SHA-256
    segments              INT NOT NULL DEFAULT 1,
    price                 BIGINT NOT NULL DEFAULT 0,
    fail_code             VARCHAR(32),
    fail_reason           VARCHAR(256),
    accepted_at           TIMESTAMP NOT NULL,
    sent_at               TIMESTAMP,
    finalized_at          TIMESTAMP,
    created_at            TIMESTAMP NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_cdr_idempotency UNIQUE (customer_id, customer_message_id)
);
```

#### bulk_jobs 테이블

```sql
CREATE TABLE bulk_jobs (
    id                    BIGSERIAL PRIMARY KEY,
    job_id                VARCHAR(64) NOT NULL UNIQUE,
    customer_id           VARCHAR(64) NOT NULL,
    template_id           VARCHAR(64),
    object_key            VARCHAR(512) NOT NULL,
    status                VARCHAR(32) NOT NULL,
    total_count           INT NOT NULL DEFAULT 0,
    success_count         INT NOT NULL DEFAULT 0,
    fail_count            INT NOT NULL DEFAULT 0,
    scheduled_at          TIMESTAMP,
    callback_url          VARCHAR(512),
    created_at            TIMESTAMP NOT NULL DEFAULT NOW(),
    started_at            TIMESTAMP,
    completed_at          TIMESTAMP
);
```

### 4.2 Redis 키 패턴

| 용도 | 키 패턴 | TTL |
|------|---------|-----|
| 멱등성 | `idem:{customerId}:{customerMessageId}` | 24시간 |
| Rate Limit | `rl:{customerId}:{yyyyMMddHHmm}` | Bucket4j 설정 |

### 4.3 Kafka 토픽

#### bulk.send.task

```json
{
  "jobId": "job-uuid-12345",
  "chunkIndex": 0,
  "customerId": "cust-001",
  "templateId": "tpl-welcome-v1",
  "datasetRef": {
    "objectKey": "cust-001/2025/01/28/abc123.jsonl.gz",
    "startOffset": 0,
    "endOffset": 10000
  }
}
```

| 항목 | 값 |
|------|-----|
| Partition Key | `jobId` |

#### cdr.events

```json
{
  "eventId": "evt-uuid-11111",
  "eventType": "DELIVERY_RESULT",
  "occurredAt": "2025-01-28T09:00:05Z",
  "payload": {
    "customerId": "cust-001",
    "receiptId": "receipt-uuid-67890",
    "customerMessageId": "msg-12345",
    "channel": "SMS",
    "status": "SENT",
    "recipientHash": "a1b2c3d4e5f6...",
    "segments": 1,
    "price": 20
  }
}
```

| 항목 | 값 |
|------|-----|
| Partition Key | `customerId` |

### 4.4 MinIO 파일 구조

| 항목 | 값 |
|------|-----|
| 형식 | JSON Lines (`.jsonl`) + gzip |
| 경로 | `{customerId}/{yyyy}/{MM}/{dd}/{uuid}.jsonl.gz` |

```json
{"customerMessageId": "msg-001", "recipient": "01012345678", "vars": {"name": "홍길동"}}
{"customerMessageId": "msg-002", "recipient": "01087654321", "vars": {"name": "김철수"}}
```

---

## 5. 핵심 정책

### 5.1 템플릿 Placeholder

| 항목 | 규칙 |
|------|------|
| 형식 | `{{variableName}}` |
| 정규식 | `\{\{([a-zA-Z_][a-zA-Z0-9_]*)\}\}` |
| 검증 | 요청 vars가 템플릿 placeholder를 **모두 포함**해야 함 |

### 5.2 전화번호 검증

- **라이브러리**: Google libphonenumber
- **입력 허용**: `01012345678`, `010-1234-5678`, `+821012345678`
- **내부 저장**: E.164 (`+821012345678`)

### 5.3 재시도 전략

| 분류 | 조건 | 처리 |
|-----|------|------|
| Retry 대상 | Timeout, 5xx, 네트워크 오류, 429 | 재시도 큐 (5s → 30s → 2m → DLQ) |
| DLQ 대상 | Payload 오류, 4xx (400, 401, 403, 404) | 즉시 DLQ |

### 5.4 CDR 이벤트 발행 전략 (v1)

| 항목 | 정책 |
|------|------|
| 발행 방식 | 동기 publish + 로컬 재시도 (backoff) |
| 실패 시 | 메시지 처리 자체를 실패로 간주 → MQ retry로 재처리 |
| 장기 Kafka 장애 | MQ에 메시지 적체 (자연스러운 버퍼링) |

**v1 허용 리스크**: "발송 성공 + CDR 이벤트 유실" 가능성 존재 (극히 드묾)
**v2 강화**: Outbox 패턴으로 at-least-once 보장

### 5.5 PII 마스킹

| 데이터 | 원본 | 마스킹 |
|--------|------|--------|
| 전화번호 | `01012345678` | `010****5678` |
| 이름 | `홍길동` | `홍*동` |
| API Key | `sk-abc123xyz` | `sk-***xyz` |

---

## 6. 에러 코드 체계

| 코드 | 설명 | HTTP | gRPC |
|------|------|------|------|
| `INVALID_REQUEST` | 요청 형식 오류 | 400 | INVALID_ARGUMENT |
| `INVALID_RECIPIENT` | 수신자 번호 오류 | 400 | INVALID_ARGUMENT |
| `TEMPLATE_NOT_FOUND` | 템플릿 없음 | 404 | NOT_FOUND |
| `RATE_LIMIT_EXCEEDED` | TPS 제한 초과 | 429 | RESOURCE_EXHAUSTED |
| `IDEMPOTENCY_CONFLICT` | 멱등 키 중복 | 409 | ALREADY_EXISTS |
| `SERVICE_UNAVAILABLE` | 인프라 장애 | 503 | UNAVAILABLE |

---

*문서 버전: 2.0*
*최종 수정일: 2025-01-28*
