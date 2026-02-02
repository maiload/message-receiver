# 아키텍처

## 시스템 구조

```
message-receiver/
├── common/         # 공통 (예외, 도메인 enum, 유틸리티)
├── receiver/       # gRPC/REST API 진입점
├── worker/         # MQ/Kafka Consumer + Gateway 발송
├── cdr-writer/     # CDR 이벤트 소비 + DB 적재
├── orchestrator/   # Bulk Job 관리 (스케줄러)
└── docker/         # 로컬 인프라
```

모든 서브모듈은 `common`에만 의존. 서브모듈 간 직접 의존 없음.

```
                ┌────────┐
                │ common │
                └───┬────┘
       ┌────────┬───┴───┬────────────┐
       ▼        ▼       ▼            ▼
  receiver   worker  cdr-writer  orchestrator
```

Mermaid 시퀀스 다이어그램은 `docs/FLOWS.md`에 정리.

### 모듈별 아키텍처

| 모듈 | 아키텍처 | 이유 |
|------|---------|------|
| receiver | 헥사고날 | 다양한 외부 의존성 (gRPC, REST, Redis, MQ, Kafka, DB) |
| worker | 헥사고날 | Gateway 연동, 재시도/실패 처리 |
| cdr-writer | 레이어드 | ETL 성격 (Consume → Transform → Write) |
| orchestrator | 레이어드 | 파일 처리 + 청크 생성, 단순 흐름 |

---

## Realtime 메시지 플로우

### Happy Case

```
Client ──gRPC Submit──▶ Receiver
                          ├─ 인증 (API Key → customer 조회)
                          ├─ Rate Limit 검사 (Bucket4j + Redis)
                          ├─ 멱등성 검사 (Redis SETNX, 24h TTL)
                          ├─ 템플릿 조회 + 변수 검증 (DB)
                          ├─ 수신자 전화번호 검증 (libphonenumber)
                          └─ RabbitMQ 발행 ──▶ receipt_id 응답

Worker (RabbitMQ Consumer)
  ├─ Gateway 호출 (외부 발송)
  └─ CDR 이벤트 발행 (Kafka cdr.events, status=SENT)

CDR Writer (Kafka Consumer)
  ├─ 마이크로 배치 (5000건 or 5초)
  └─ DB 배치 삽입 (ON CONFLICT DO NOTHING)
```

### Error Cases

```
[인증 실패]
  API Key 누락/불일치/만료 → UNAUTHENTICATED 즉시 반환

[Rate Limit 초과]
  Bucket4j 토큰 소진 → RESOURCE_EXHAUSTED 반환

[멱등 중복]
  Redis에 동일 키 존재 → 기존 receipt_id + idempotency_hit=true 반환

[템플릿 오류]
  템플릿 없음/변수 불일치 → INVALID_ARGUMENT 또는 NOT_FOUND 반환

[Gateway 발송 실패]
  Timeout/5xx → 재시도 큐 (5s → 30s → 2m → DLQ)
  4xx 영구 실패 → 즉시 DLQ
  Circuit Breaker OPEN → 일시 차단 후 자동 복구

[CDR 발행 실패]
  Kafka 발행 실패 → MQ retry로 전체 재처리
```

### 재시도 큐 구성

```
Main Queue ──실패──▶ Retry-5s ──TTL──▶ Main Queue
                          ↓ (2차 실패)
                     Retry-30s ──TTL──▶ Main Queue
                          ↓ (3차 실패)
                     Retry-2m ──TTL──▶ Main Queue
                          ↓ (최종 실패)
                         DLQ
```

---

## Bulk 메시지 플로우

### Happy Case

```
Client ──REST POST /bulk/jobs──▶ Receiver
                                   ├─ 인증 (API Key)
                                   ├─ Job 생성 (DB, status=PENDING)
                                   └─ 201 Created + jobId 응답

Orchestrator (@Scheduled 5초 폴링)
  ├─ PENDING Job 조회 (scheduledAt 도래 여부 확인)
  ├─ Job 상태 → PROCESSING
  ├─ MinIO 파일 스트리밍 (JSONL + gzip)
  ├─ 청크 분할 + Kafka 발행 (bulk.send.task)
  └─ Job 상태 → PUBLISHED (total_count 확정)

Worker (Kafka Consumer)
  ├─ 청크 내 각 메시지:
  │   ├─ send_attempts INSERT (중복 방지, ON CONFLICT → skip)
  │   ├─ 템플릿 렌더링
  │   └─ Gateway 호출
  ├─ 성공 → CDR 이벤트 (status=SENT)
  ├─ 실패 → CDR 이벤트 (status=FAILED)
  └─ 중복 skip → CDR 이벤트 (status=SKIPPED)

CDR Writer (Kafka Consumer)
  ├─ SENT/FAILED → cdr_records 배치 삽입
  ├─ SKIPPED → cdr_records에 미삽입 (count 집계만)
  └─ bulk_jobs 카운트 증가 (success/fail/skip)
      └─ 전체 처리 완료 시 최종 상태 결정:
          ├─ 전부 성공 → COMPLETED
          ├─ 일부 성공 → PARTIALLY_COMPLETED
          ├─ 전부 실패 → FAILED
          └─ 전부 스킵 → SKIPPED
```

### Error Cases

```
[파일 오류]
  MinIO 파일 없음/읽기 실패 → Job status=FAILED

[중복 발송 방지]
  send_attempts 테이블 UNIQUE 제약 → 중복 customerMessageId는 skip
  (Realtime은 Redis 멱등성 검사로 Receiver에서 사전 차단)

[Gateway 발송 실패]
  Worker에서 CDR(FAILED) 발행 → cdr-writer가 fail_count 증가

[부분 실패]
  일부만 성공 시 → PARTIALLY_COMPLETED
  fail_count + skip_count = total_count → FAILED
```

---

## 도메인 타입

| Enum | 값 | 용도 |
|------|-----|------|
| `ChannelType` | SMS, LMS, MMS | 메시지 채널 |
| `SendType` | REALTIME, BULK | 발송 유형 |
| `MessageStatus` | SENT, FAILED, SKIPPED | 개별 메시지 발송 결과 |
| `JobStatus` | PENDING, PROCESSING, PUBLISHED, COMPLETED, PARTIALLY_COMPLETED, FAILED, SKIPPED | Bulk Job 상태 |

### 경계별 변환

| 경계 | 방식 |
|------|------|
| DB (jOOQ) | `forcedType` + `EnumConverter` → VARCHAR ↔ Enum 자동 변환 |
| JSON (Kafka/RabbitMQ) | Jackson 자동 (`enum.name()` ↔ `Enum.valueOf()`) |
| gRPC (Protobuf) | Adapter에서 명시적 매핑 (Proto enum ↔ Domain enum) |

---

## 패키지 구조

### 헥사고날 (receiver, worker)

```
<module>/
├── application/
│   ├── port/
│   │   ├── in/         # Inbound Port 인터페이스 (+ 내부 DTO record)
│   │   └── out/        # Outbound Port 인터페이스
│   └── service/        # Inbound Port 구현체
└── adapter/
    ├── in/             # gRPC, REST, Consumer
    └── out/            # Redis, MQ, Kafka, DB
```

### 레이어드 (cdr-writer, orchestrator)

```
<module>/
├── config/
├── consumer/           # Kafka Consumer (또는 scheduler)
├── service/
└── repository/
```

### 명명 규칙

| 유형 | Port | Adapter |
|------|------|---------|
| 메시지 큐 | `{기능}QueuePort` | `Rabbit{기능}QueueAdapter` |
| 이벤트 발행 | `{기능}PublisherPort` | `Kafka{기능}PublisherAdapter` |
| 저장소 | `{도메인}RepositoryPort` | `Jooq{도메인}RepositoryAdapter` |
| 외부 API | `{대상}GatewayPort` | `Http{대상}GatewayAdapter` |

---

## 예외 계층

```
BaseException (extends RuntimeException)
├── DomainException
│   ├── ValidationException
│   └── PolicyViolationException
└── InfrastructureException
    ├── RedisException
    ├── MessagingException
    └── GatewayException
```
