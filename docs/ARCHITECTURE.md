# 아키텍처 & 개발 가이드

## 1. 기술 스택

### 1.1 Core

| 기술 | 버전 | 용도 |
|-----|------|------|
| Java | 21 | 메인 언어 |
| Spring Boot | 4.0.2 | 프레임워크 |
| Gradle | Groovy DSL | 빌드 |
| gRPC | spring-grpc 1.0.1 | Realtime API |
| RabbitMQ | Spring AMQP | 실시간 메시지 큐 |
| Kafka | Spring Kafka | Bulk/CDR 이벤트 |
| Redis | Lettuce | 멱등성, Rate Limit |
| PostgreSQL | 17 | CDR 저장소 |
| MinIO | S3 호환 | Bulk 파일 저장 |

### 1.2 라이브러리

| 라이브러리 | 버전 | 용도 |
|-----------|------|------|
| Bucket4j | 8.16.0 | Rate Limiting (Token Bucket) |
| Resilience4j | - | Circuit Breaker, Timeout |
| jOOQ | 3.19.29 | DB 접근 (forcedType + EnumConverter) |
| libphonenumber | - | 전화번호 검증 |
| Micrometer + Prometheus | - | 메트릭 |
| Flyway | - | DB 마이그레이션 |

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
           ┌───────────┬───┴───┬───────────┐
           ▼           ▼       ▼           ▼
    ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────────┐
    │ receiver │ │  worker  │ │cdr-writer│ │ orchestrator │
    └──────────┘ └──────────┘ └──────────┘ └──────────────┘
```

> 모든 서브모듈은 `common`에만 의존합니다. 서브모듈 간 직접 의존은 없습니다.

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

외부 의존성이 다양한 모듈에 적용

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

### 4.2 Inbound Port & Service

**Port 규칙**: `{도메인}Port`
**Service 규칙**: `{도메인}Service`
**DTO 규칙**: Port 인터페이스 내부에 `{동사}`, `{동사}Result` record로 정의

```
# application/port/in/
RealtimeMessagePort             # submit(), getReceiptStatus()
BulkJobPort                     # create(), getStatus()

# application/service/
RealtimeMessageService (implements RealtimeMessagePort)
BulkJobService (implements BulkJobPort)
```

### 4.3 Port 내부 DTO 예시

```java
public interface RealtimeMessagePort {

    SubmitResult submit(Submit submit);

    ReceiptStatus getReceiptStatus(String customerId, String receiptId);

    record Submit(
        String customerId,
        String customerMessageId,
        String messageType,
        String recipient,
        String templateId,
        String content,
        Map<String, String> vars,
        Integer ttlSeconds,
        List<String> mediaUrls
    ) {}

    record SubmitResult(
        String receiptId,
        LocalDateTime acceptedAt,
        boolean idempotencyHit
    ) {}

    record ReceiptStatus(
        String receiptId,
        String customerMessageId,
        MessageStatus status,
        String failCode,
        String failReason,
        LocalDateTime acceptedAt,
        LocalDateTime sentAt
    ) {}
}
```

---

## 5. 도메인 타입

### 5.1 도메인 Enum (common 모듈)

모든 상태값은 `common` 모듈에 enum으로 정의하여 타입 안전성을 보장합니다.

| Enum | 값 | 용도 |
|------|-----|------|
| `ChannelType` | SMS, LMS, MMS | 메시지 채널 (maxLength 포함) |
| `SendType` | REALTIME, BULK | 발송 유형 |
| `MessageStatus` | SENT, FAILED | 발송 결과 |
| `JobStatus` | PENDING, PROCESSING, PUBLISHED, DELIVERED, FAILED | Bulk Job 상태 |

### 5.2 경계(Boundary)별 변환 전략

| 경계 | 변환 방식 |
|------|----------|
| DB (jOOQ) | `forcedType` + `EnumConverter` → 자동 변환 (VARCHAR ↔ Enum) |
| JSON (Kafka/RabbitMQ) | Jackson 자동 직렬화/역직렬화 (`enum.name()` ↔ `Enum.valueOf()`) |
| gRPC (Protobuf) | Adapter에서 명시적 매핑 (Proto enum ↔ Domain enum) |

> Proto는 자체 타입을 생성하므로 common enum을 직접 사용할 수 없습니다.
> 예: `DeliveryStatus` (proto) ↔ `MessageStatus` (domain)

### 5.3 시간 타입

모든 시간 필드는 `LocalDateTime`으로 통일합니다 (Port DTO, DB, 내부 로직 모두 동일).

---

## 6. 예외 처리

### 6.1 예외 계층

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

### 6.2 ErrorCode Enum

```java
public enum ErrorCode {
    // Authentication
    UNAUTHENTICATED("A001", false),
    PERMISSION_DENIED("A002", false),
    API_KEY_EXPIRED("A003", false),

    // Validation
    INVALID_REQUEST("V001", false),
    INVALID_RECIPIENT("V002", false),
    INVALID_TEMPLATE("V003", false),
    TEMPLATE_NOT_FOUND("V004", false),
    RECEIPT_NOT_FOUND("V005", false),
    JOB_NOT_FOUND("V006", false),

    // Policy
    RATE_LIMIT_EXCEEDED("P001", true),
    IDEMPOTENCY_CONFLICT("P002", false),

    // Infrastructure
    REDIS_CONNECTION_FAILED("I001", true),
    MQ_PUBLISH_FAILED("I002", true),
    KAFKA_PUBLISH_FAILED("I003", true),
    DB_CONNECTION_FAILED("I004", true),

    // Gateway
    GATEWAY_TIMEOUT("G001", true),
    GATEWAY_5XX("G002", true),
    GATEWAY_4XX_PERMANENT("G003", false);

    private final String code;
    private final boolean retryable;
}
```

---

## 7. 설정 관리

### 7.1 Profile

| Profile | 용도 | 인프라 |
|---------|------|--------|
| `local` | 로컬 개발 | Docker Compose |
| `dev` | 개발 서버 | 개발 인프라 |
| `staging` | 스테이징 | 스테이징 인프라 |
| `prod` | 운영 | 운영 인프라 |

### 7.2 주요 설정값

```yaml
receiver:
  # REST: 8081 (Tomcat), gRPC: 9090 (Netty)
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

## 8. 테스트 전략

### 8.1 테스트 레이어

| 레이어 | 범위 | 도구 |
|--------|------|------|
| Unit | Service 로직, 유틸리티 | JUnit 5, Mockito |
| Integration | Adapter 단위 | Spring Boot Test |
| E2E | 전체 흐름 (선택) | Docker Compose |

---

## 9. 로깅 & 추적

### 9.1 MDC 표준 키

| 키 | 설명 | 필수 |
|----|------|------|
| `traceId` | 분산 추적 ID | O |
| `customerId` | 고객 ID | O |
| `receiptId` | 영수증 ID | - |
| `jobId` | Job ID (Bulk) | - |

### 9.2 로그 레벨 가이드

| 레벨 | 용도 |
|------|------|
| ERROR | 예외 발생, 처리 실패 |
| WARN | 예상된 실패 (rate limit, 멱등 중복) |
| INFO | 주요 처리 완료 (수신, 발송) |
| DEBUG | 상세 처리 흐름 |

---

## 10. Gradle 설정

### 10.1 settings.gradle

```groovy
rootProject.name = 'message-receiver'

include 'common'
include 'receiver'
include 'worker'
include 'cdr-writer'
include 'orchestrator'
```

### 10.2 build.gradle (루트)

```groovy
plugins {
    id 'java'
    id 'org.springframework.boot' version '4.0.2' apply false
    id 'io.spring.dependency-management' version '1.1.7' apply false
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
        toolchain {
            languageVersion = JavaLanguageVersion.of(21)
        }
    }

    dependencyManagement {
        imports {
            mavenBom 'org.springframework.boot:spring-boot-dependencies:4.0.2'
        }
    }

    configurations {
        compileOnly {
            extendsFrom annotationProcessor
        }
    }

    dependencies {
        compileOnly 'org.projectlombok:lombok'
        annotationProcessor 'org.projectlombok:lombok'

        testImplementation 'org.springframework.boot:spring-boot-starter-test'
        testRuntimeOnly 'org.junit.platform:junit-platform-launcher'
    }

    tasks.named('test') {
        useJUnitPlatform()
    }
}
```

### 10.3 receiver/build.gradle

```groovy
plugins {
    id 'org.springframework.boot'
    id 'com.google.protobuf' version '0.9.5'
    id 'nu.studer.jooq' version '10.2'
}

ext {
    set('springGrpcVersion', '1.0.1')
}

dependencies {
    implementation project(':common')

    // Spring Boot
    implementation 'org.springframework.boot:spring-boot-starter-webmvc'
    implementation 'org.springframework.boot:spring-boot-starter-validation'

    // gRPC
    implementation 'io.grpc:grpc-services'
    implementation 'org.springframework.grpc:spring-grpc-server-web-spring-boot-starter'

    // Redis
    implementation 'org.springframework.boot:spring-boot-starter-data-redis'

    // RabbitMQ
    implementation 'org.springframework.boot:spring-boot-starter-amqp'

    // Rate Limiting
    implementation 'com.bucket4j:bucket4j_jdk17-redis-common:8.16.0'
    implementation 'com.bucket4j:bucket4j_jdk17-lettuce:8.16.0'

    // Database
    implementation 'org.springframework.boot:spring-boot-starter-jooq'
    implementation 'org.springframework.boot:spring-boot-starter-flyway'
    implementation 'org.flywaydb:flyway-database-postgresql'
    runtimeOnly 'org.postgresql:postgresql'
    jooqGenerator 'org.postgresql:postgresql:42.7.8'

    // Observability
    implementation 'org.springframework.boot:spring-boot-starter-actuator'
    runtimeOnly 'io.micrometer:micrometer-registry-prometheus'

    // Test
    testImplementation 'org.springframework.boot:spring-boot-starter-amqp-test'
    testImplementation 'org.springframework.grpc:spring-grpc-test'
}

dependencyManagement {
    imports {
        mavenBom "org.springframework.grpc:spring-grpc-dependencies:${springGrpcVersion}"
    }
}
```

### 10.4 jOOQ forcedType 설정

모든 DB 접근 모듈(receiver, cdr-writer, orchestrator)에 동일하게 적용됩니다.

```groovy
jooq {
    version = '3.19.29'
    edition = nu.studer.gradle.jooq.JooqEdition.OSS

    configurations {
        main {
            generateSchemaSourceOnCompilation = false

            generationTool {
                // ...jdbc 설정 생략...
                generator {
                    database {
                        name = 'org.jooq.meta.postgres.PostgresDatabase'
                        inputSchema = 'messaging'
                        forcedTypes {
                            forcedType {
                                userType = 'com.maiload.messagereceiver.common.domain.JobStatus'
                                enumConverter = true
                                includeExpression = 'messaging\\.bulk_jobs\\.status'
                            }
                            forcedType {
                                userType = 'com.maiload.messagereceiver.common.domain.MessageStatus'
                                enumConverter = true
                                includeExpression = 'messaging\\.cdr_records\\.status'
                            }
                            forcedType {
                                userType = 'com.maiload.messagereceiver.common.domain.SendType'
                                enumConverter = true
                                includeExpression = 'messaging\\.cdr_records\\.send_type'
                            }
                            forcedType {
                                userType = 'com.maiload.messagereceiver.common.domain.ChannelType'
                                enumConverter = true
                                includeExpression = 'messaging\\.cdr_records\\.channel'
                            }
                        }
                    }
                }
            }
        }
    }
}
```

> `forcedType` + `enumConverter = true`로 jOOQ가 `EnumConverter`를 생성합니다.
> 이를 통해 Repository에서 `.name()`/`valueOf()` 변환 없이 enum을 직접 사용할 수 있습니다.

---

*문서 버전: 3.0*
*최종 수정일: 2025-01-29*
