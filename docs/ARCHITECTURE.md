# 아키텍처 & 개발 가이드

## 1. 기술 스택

### 1.1 Core

| 기술 | 버전 | 용도 |
|-----|------|------|
| Java | 21 | 메인 언어 |
| Spring Boot | 4.0.2 | 프레임워크 |
| Gradle | Groovy DSL | 빌드 |
| gRPC | grpc-spring-boot-starter 3.x | Realtime API |
| RabbitMQ | Spring AMQP | 실시간 메시지 큐 |
| Kafka | Spring Kafka | Bulk/CDR 이벤트 |
| Redis | Lettuce | 멱등성, Rate Limit |
| PostgreSQL | 16 | CDR 저장소 |
| MinIO | S3 호환 | Bulk 파일 저장 |

### 1.2 라이브러리

| 라이브러리 | 용도 |
|-----------|------|
| Bucket4j | Rate Limiting (Token Bucket) |
| Resilience4j | Circuit Breaker, Timeout |
| jOOQ | 배치 DB 접근 |
| libphonenumber | 전화번호 검증 |
| Micrometer + Prometheus | 메트릭 |
| OpenTelemetry SDK | 분산 추적 |

---

## 2. 멀티모듈 구조

```
message-receiver/
├── build.gradle                  # 루트 빌드
├── settings.gradle               # 모듈 정의
├── common/                       # 공통 모듈
├── receiver/                     # gRPC/REST API
├── worker/                       # MQ Consumer + Gateway
├── cdr-writer/                   # CDR Consumer + DB
├── orchestrator/                 # Bulk Job 관리
└── docker/                       # 로컬 인프라
```

### 2.1 모듈 의존성

```
                    ┌─────────────┐
                    │   common    │
                    └──────┬──────┘
           ┌───────────────┼───────────────┐
           ▼               ▼               ▼
    ┌─────────────┐ ┌─────────────┐ ┌─────────────┐
    │  receiver   │ │   worker    │ │ cdr-writer  │
    └─────────────┘ └─────────────┘ └─────────────┘
           │
    ┌──────▼──────┐
    │ orchestrator│
    └─────────────┘
```

---

## 3. 패키지 구조

### 3.1 아키텍처 적용 기준

| 모듈 | 아키텍처 | 근거 |
|------|---------|------|
| `receiver` | 헥사고날 | 핵심 비즈니스 로직, 다양한 외부 의존성 |
| `worker` | 헥사고날 | Gateway 연동, 재시도/실패 처리 복잡 |
| `cdr-writer` | 단순 레이어드 | ETL 성격, 단순 Consume → Transform → Write |
| `orchestrator` | 단순 레이어드 | 파일 처리 + 청크 생성, 복잡한 도메인 로직 없음 |

### 3.2 헥사고날 아키텍처 (receiver, worker)

핵심 도메인 로직이 있고, 외부 의존성이 다양한 모듈에 적용

```
<module>/{domain}
├── domain/
│   ├── model/          # Entity, Value Object
│   ├── policy/         # 도메인 규칙
│   └── event/          # 도메인 이벤트
├── application/
│   ├── usecase/        # UseCase 인터페이스/구현
│   ├── command/        # Write DTO
│   └── mapper/         # 도메인 ↔ DTO
├── port/
│   ├── in/             # UseCase 인터페이스
│   └── out/            # 외부 의존 인터페이스
└── adapter/
    ├── in/             # gRPC, REST, Consumer
    └── out/            # Redis, MQ, Kafka, DB
```

### 3.3 단순 레이어드 (cdr-writer, orchestrator)

ETL/배치 성격의 모듈에 적용. 과잉 설계 방지.

```
<module>/
├── config/             # 설정
├── consumer/           # Kafka Consumer (또는 scheduler)
├── service/            # 비즈니스 로직
└── repository/         # DB 접근
```

> **원칙**: 헥사고날을 "모든 곳에 동일 강도로" 적용하면 과잉.
> 복잡한 도메인이 없으면 단순 레이어드로 충분.

---

## 4. 명명 규칙

### 4.1 Port & Adapter

| 유형 | Port 명명 | Adapter 명명 |
|------|----------|-------------|
| 외부 시스템 | `{기능}Port` | `{기술}{기능}Adapter` |
| 메시지 큐 | `{기능}QueuePort` | `Rabbit{기능}QueueAdapter` |
| 이벤트 발행 | `{기능}PublisherPort` | `Kafka{기능}PublisherAdapter` |
| 저장소 | `{도메인}RepositoryPort` | `Jooq{도메인}RepositoryAdapter` |
| 외부 API | `{대상}GatewayPort` | `Http{대상}GatewayAdapter` |

### 4.2 UseCase

**규칙**: `{동사}{목적어}UseCase`

```
SubmitRealtimeMessageUseCase
ConsumeRealtimeTaskUseCase
WriteCdrBatchUseCase
```

---

## 5. 예외 처리

### 5.1 예외 계층

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

### 5.2 ErrorCode Enum

```java
public enum ErrorCode {
    // Validation
    INVALID_REQUEST("V001", false),
    INVALID_RECIPIENT("V003", false),

    // Policy
    RATE_LIMIT_EXCEEDED("P001", true),
    IDEMPOTENCY_CONFLICT("P002", false),

    // Infrastructure
    REDIS_CONNECTION_FAILED("I001", true),
    MQ_PUBLISH_FAILED("I002", true),

    // Gateway
    GATEWAY_TIMEOUT("G001", true),
    GATEWAY_5XX("G002", true),
    GATEWAY_4XX_PERMANENT("G003", false);

    private final String code;
    private final boolean retryable;
}
```

---

## 6. 설정 관리

### 6.1 Profile

| Profile | 용도 | 인프라 |
|---------|------|--------|
| `local` | 로컬 개발 | Docker Compose |
| `dev` | 개발 서버 | 개발 인프라 |
| `staging` | 스테이징 | 스테이징 인프라 |
| `prod` | 운영 | 운영 인프라 |

### 6.2 주요 설정값

```yaml
receiver:
  grpc:
    port: 9090
    max-concurrent-calls: 1000
  rate-limit:
    default-tps: 100
  idempotency:
    ttl-seconds: 86400         # 24시간

worker:
  mq:
    concurrency: 50
    prefetch-count: 10
  gateway:
    timeout-ms: 3000
    concurrency-limit: 200
  circuit-breaker:
    failure-rate-threshold: 50
    wait-duration-in-open-state-ms: 30000
  retry:
    delays-seconds: [5, 30, 120]

cdr-writer:
  batch:
    max-size: 5000
    flush-interval-ms: 5000
```

---

## 7. 테스트 전략

### 7.1 테스트 레이어

| 레이어 | 범위 | 도구 |
|--------|------|------|
| Unit | domain policy, usecase 로직 | JUnit 5, Mockito |
| Integration | Adapter 단위 | Testcontainers |
| E2E | 전체 흐름 (선택) | Docker Compose |

### 7.2 Testcontainers 대상

- Redis
- RabbitMQ
- Kafka
- PostgreSQL
- MinIO

---

## 8. 로깅 & 추적

### 8.1 MDC 표준 키

| 키 | 설명 | 필수 |
|----|------|------|
| `traceId` | 분산 추적 ID | O |
| `customerId` | 고객 ID | O |
| `receiptId` | 영수증 ID | - |
| `jobId` | Job ID (Bulk) | - |

### 8.2 로그 레벨 가이드

| 레벨 | 용도 |
|------|------|
| ERROR | 예외 발생, 처리 실패 |
| WARN | 예상된 실패 (rate limit, 멱등 중복) |
| INFO | 주요 처리 완료 (수신, 발송) |
| DEBUG | 상세 처리 흐름 |

---

## 9. Gradle 설정

### 9.1 settings.gradle

```groovy
rootProject.name = 'message-receiver'

include 'common'
include 'receiver'
include 'worker'
include 'cdr-writer'
include 'orchestrator'
```

### 9.2 build.gradle (루트)

```groovy
plugins {
    id 'java'
    id 'org.springframework.boot' version '4.0.2' apply false
    id 'io.spring.dependency-management' version '1.1.4' apply false
}

allprojects {
    group = 'com.maiload'
    version = '1.0.0-SNAPSHOT'

    repositories {
        mavenCentral()
    }
}

subprojects {
    apply plugin: 'java'
    apply plugin: 'io.spring.dependency-management'

    java {
        sourceCompatibility = JavaVersion.VERSION_21
        targetCompatibility = JavaVersion.VERSION_21
    }
}
```

### 9.3 receiver/build.gradle

```groovy
plugins {
    id 'org.springframework.boot'
}

dependencies {
    implementation project(':common')
    implementation 'org.springframework.boot:spring-boot-starter'
    implementation 'org.springframework.boot:spring-boot-starter-web'
    implementation 'net.devh:grpc-spring-boot-starter:3.1.0.RELEASE'
    implementation 'org.springframework.boot:spring-boot-starter-data-redis'
    implementation 'org.springframework.boot:spring-boot-starter-amqp'
    implementation 'com.bucket4j:bucket4j-redis:8.10.1'
    implementation 'org.springframework.boot:spring-boot-starter-actuator'
    implementation 'io.micrometer:micrometer-registry-prometheus'

    testImplementation 'org.springframework.boot:spring-boot-starter-test'
    testImplementation 'org.testcontainers:junit-jupiter'
}
```

---

*문서 버전: 2.0*
*최종 수정일: 2025-01-28*
