# 운영 & 개발환경 가이드

## 1. 로컬 개발 환경

### 1.1 필수 소프트웨어

| 소프트웨어 | 버전 |
|-----------|------|
| JDK | 21+ |
| Gradle | 8.x |
| Docker | 24+ |
| Docker Compose | 2.x |

### 1.2 Docker Compose 서비스

| 서비스 | 이미지 | 포트 |
|--------|--------|------|
| redis | redis:7-alpine | 6379 |
| rabbitmq | rabbitmq:4-management | 5672, 15672 |
| kafka | apache/kafka:3.9.0 | 9094 |
| postgres | postgres:17 | 5432 |
| minio | minio/minio:latest | 9000, 9001 |
| kafka-ui | provectuslabs/kafka-ui:latest | 8080 |

### 1.3 docker-compose.yml

```yaml
services:
  postgres:
    image: postgres:17
    container_name: message-receiver-postgres
    environment:
      POSTGRES_USER: maiload
      POSTGRES_PASSWORD: maiload
      POSTGRES_DB: message_receiver
    ports:
      - "5432:5432"

  redis:
    image: redis:7-alpine
    container_name: message-receiver-redis
    ports:
      - "6379:6379"

  rabbitmq:
    image: rabbitmq:4-management
    container_name: message-receiver-rabbitmq
    environment:
      RABBITMQ_DEFAULT_USER: maiload
      RABBITMQ_DEFAULT_PASS: maiload
    ports:
      - "5672:5672"
      - "15672:15672"

  kafka:
    image: apache/kafka:3.9.0
    container_name: message-receiver-kafka
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093,EXTERNAL://:9094
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092,EXTERNAL://localhost:9094
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,EXTERNAL:PLAINTEXT
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      CLUSTER_ID: "MkU3OEVBNTcwNTJENDM2Qk"
    ports:
      - "9094:9094"

  minio:
    image: minio/minio:latest
    container_name: message-receiver-minio
    environment:
      MINIO_ROOT_USER: maiload
      MINIO_ROOT_PASSWORD: maiload123
    command: server /data --console-address ":9001"
    ports:
      - "9000:9000"
      - "9001:9001"

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    container_name: message-receiver-kafka-ui
    environment:
      KAFKA_CLUSTERS_0_NAME: local
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:9092
    ports:
      - "8080:8080"
```

### 1.4 실행 방법

```bash
# 인프라 시작
cd docker
docker-compose up -d

# Kafka 토픽 생성
docker exec message-receiver-kafka /opt/kafka/bin/kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic bulk.send.task --partitions 6 --if-not-exists

docker exec message-receiver-kafka /opt/kafka/bin/kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic cdr.events --partitions 6 --if-not-exists

# 애플리케이션 실행
./gradlew :receiver:bootRun --args='--spring.profiles.active=local'
./gradlew :worker:bootRun --args='--spring.profiles.active=local'
./gradlew :cdr-writer:bootRun --args='--spring.profiles.active=local'
```

### 1.5 관리 UI

| 서비스 | URL | 계정 |
|--------|-----|------|
| RabbitMQ | http://localhost:15672 | maiload / maiload |
| MinIO | http://localhost:9001 | maiload / maiload123 |
| Kafka UI | http://localhost:8080 | - |

---

## 2. 모니터링

### 2.1 핵심 메트릭

#### Ingress (Receiver)

| 메트릭 | 설명 |
|--------|------|
| `receiver.request.tps` | 초당 요청 수 |
| `receiver.request.latency.p95` | 요청 처리 지연 (p95) |
| `receiver.ratelimit.rejected` | Rate limit 거부 수 |

#### Worker

| 메트릭 | 설명 |
|--------|------|
| `gateway.request.success.rate` | 발송 성공률 |
| `gateway.request.latency.p95` | Gateway 호출 지연 |
| `gateway.circuit.state` | 서킷 브레이커 상태 |

#### Queue

| 메트릭 | 설명 |
|--------|------|
| `mq.queue.depth` | 메인 큐 대기 수 |
| `mq.dlq.count` | DLQ 적재 수 |
| `kafka.consumer.lag` | Kafka Consumer 지연 |

### 2.2 대시보드

```
┌─────────────────────────────────────────────────────────────┐
│  Message Receiver Dashboard                                  │
├─────────────────────────────────────────────────────────────┤
│  [Ingress]           [Queue]            [Gateway]           │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐     │
│  │ TPS: 1,234  │    │ Depth: 5K   │    │ Success: 99%│     │
│  │ p95: 12ms   │    │ DLQ: 23     │    │ p95: 150ms  │     │
│  └─────────────┘    └─────────────┘    └─────────────┘     │
│                                                              │
│  [System Health]                                            │
│  ┌─────────────────────────────────────────────────────┐    │
│  │ Redis: ✓  MQ: ✓  Kafka: ✓  DB: ✓                    │    │
│  └─────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────┘
```

---

## 3. 알람 설정

### 3.1 Critical (즉시 대응)

| 조건 | 임계치 |
|------|--------|
| DLQ 증가율 상승 | 5분간 연속 증가 |
| Gateway timeout rate | > 5% (5분) |
| CDR consumer lag | > 100,000 (10분) |
| Circuit breaker OPEN | 상태 변경 즉시 |

### 3.2 Warning (모니터링)

| 조건 | 임계치 |
|------|--------|
| Gateway timeout rate | > 2% (5분) |
| MQ queue depth | > 100,000 (10분) |
| Rate limit rejected | > 1% (5분) |

---

## 4. 재시도 & DLQ

### 4.1 재시도 큐 구성

```
Main Queue ──실패──▶ Retry-5s ──TTL──▶ Main Queue
                          ↓ (2차 실패)
                     Retry-30s ──TTL──▶ Main Queue
                          ↓ (3차 실패)
                     Retry-2m ──TTL──▶ Main Queue
                          ↓ (최종 실패)
                         DLQ
```

### 4.2 DLQ 운영

| 항목 | 정책 |
|------|------|
| 보관 기간 | 7일 |
| 자동 재처리 | 비활성화 (수동 검토) |
| 알람 | DLQ 적재 시 즉시 |

---

## 5. 장애 대응

### 5.1 Redis 장애

| 영향 | 대응 |
|------|------|
| 멱등성 검사 불가 | DB 유니크 제약으로 보장 |
| Rate limit 불가 | 설정에 따라 bypass 또는 reject |

### 5.2 RabbitMQ 장애

| 영향 | 대응 |
|------|------|
| 메시지 수신 불가 | gRPC UNAVAILABLE 반환 |
| 처리 지연 | Consumer 증설 |

### 5.3 Gateway 장애

| 영향 | 대응 |
|------|------|
| timeout 급증 | Circuit breaker 자동 작동 |
| 전체 장애 | MQ 적체, 복구 후 자동 재처리 |

---

## 6. 배포

### 6.1 무중단 배포

- **Rolling Update** 권장
- Receiver: gRPC graceful shutdown
- Worker: MQ Consumer graceful shutdown
- CDR Writer: Kafka offset commit 후 종료

### 6.2 롤백 절차

1. 배포 중단
2. 이전 버전 이미지로 롤백
3. 상태 확인 (메트릭, 로그)
4. 원인 분석

---

## 7. 개발 로드맵

### Phase 1: 기반 구축

- [ ] Gradle 멀티모듈 설정
- [ ] Docker Compose + 인프라
- [ ] common 모듈 (예외, 유틸리티, PiiMasker)
- [ ] DB 스키마 (Flyway)

### Phase 2: Realtime MVP

- [ ] gRPC Submit API
- [ ] 멱등성 검사 (Redis)
- [ ] Rate Limit (Bucket4j)
- [ ] RabbitMQ 발행
- [ ] API Key 인증 + 만료 검사

### Phase 3: Worker + Gateway

- [ ] RabbitMQ Consumer
- [ ] Gateway 호출 (Mock)
- [ ] Resilience4j (Timeout, CircuitBreaker)
- [ ] 재시도/DLQ
- [ ] cdr.events Kafka 발행 (동기 + 로컬 재시도)

### Phase 4: CDR Writer

- [ ] Kafka Consumer
- [ ] 마이크로 배치 (5000건 or 5초)
- [ ] jOOQ 배치 삽입
- [ ] ON CONFLICT DO NOTHING

### Phase 5: Bulk

- [ ] REST API (Job 생성/조회)
- [ ] MinIO 파일 처리
- [ ] Orchestrator (청크 생성)
- [ ] bulk.send.task Kafka 발행

### Phase 6: 마무리

- [ ] 웹훅/콜백 (선택)
- [ ] MMS URL 지원
- [ ] E2E 테스트
- [ ] 문서 정리

### 의존성 다이어그램

```
Phase 1 (기반)
    ↓
Phase 2 (Realtime)
    ↓
Phase 3 (Worker)
    ├─────────────┐
    ↓             ↓
Phase 4 (CDR)  Phase 5 (Bulk)
    └─────┬───────┘
          ↓
    Phase 6 (마무리)
```

---

*문서 버전: 2.0*
*최종 수정일: 2025-01-28*
