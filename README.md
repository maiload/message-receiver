# Message Receiver

메시징 시스템 - 실시간(Realtime) 및 대량(Bulk) 메시지 발송 처리

## 모듈 구조

| 모듈 | 설명 |
|------|------|
| `common` | 공통 예외, 유틸리티 |
| `receiver` | gRPC/REST API 진입점 |
| `worker` | MQ Consumer + Gateway 발송 |
| `cdr-writer` | CDR 이벤트 소비 + DB 적재 |
| `orchestrator` | Bulk Job 관리 |

## 기술 스택

- Java 21
- Spring Boot 4.0.2
- Spring gRPC 1.x
- RabbitMQ, Kafka
- PostgreSQL 17, Redis, MinIO
- jOOQ 3.19.29, Flyway
