# 로컬 테스트 가이드

## 사전 준비

- Docker / Docker Compose
- JDK 21
- Postman (REST + gRPC 테스트)

Postman Import: `docs/postman/message-receiver.postman_collection.json`, `docs/postman/message-receiver-local.postman_environment.json`

---

## 1. 인프라 기동

```bash
cd docker
docker-compose up -d
docker-compose ps   # 모든 서비스 healthy 확인
```

| 서비스 | 포트 | 관리 UI |
|--------|------|---------|
| PostgreSQL | 5432 | - |
| Redis | 6379 | - |
| RabbitMQ | 5672 | http://localhost:15672 (maiload/maiload) |
| Kafka | 9094 | http://localhost:8080 (Kafka UI) |
| MinIO | 9000 | http://localhost:9001 (maiload/maiload123) |

## 2. Kafka 토픽 생성

```bash
docker exec message-receiver-kafka bash /opt/kafka/init/create-topics.sh
```

## 3. MinIO 버킷 생성

```bash
docker exec message-receiver-minio mc alias set local http://localhost:9000 maiload maiload123
docker exec message-receiver-minio mc mb local/message-receiver
```

또는 MinIO Console(http://localhost:9001)에서 `message-receiver` 버킷 생성.

## 4. 애플리케이션 기동

**receiver를 가장 먼저 기동** (Flyway가 DB 스키마 + 시드 데이터 자동 적용).

```bash
./gradlew :receiver:bootRun      # 터미널 1 (먼저)
./gradlew :worker:bootRun        # 터미널 2
./gradlew :cdr-writer:bootRun    # 터미널 3
./gradlew :orchestrator:bootRun  # 터미널 4
```

| 모듈 | REST 포트 | gRPC 포트 |
|------|-----------|-----------|
| receiver | 8081 | 9090 |
| worker | 8082 | - |
| cdr-writer | 8083 | - |
| orchestrator | 8084 | - |

> 시드 데이터: 고객 `cust-001` (API Key: `test-api-key`), 템플릿 `tpl-hello`, `tpl-simple`

---

## 5. Realtime 테스트 (gRPC)

Postman > New > gRPC > URL: `localhost:9090` > Metadata: `x-api-key` = `test-api-key`

**Submit** (`RealtimeMessageService/Submit`):

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

**GetReceiptStatus** (`RealtimeMessageService/GetReceiptStatus`):

```json
{
  "customer_id": "cust-001",
  "receipt_id": "<Submit 응답의 receipt_id>"
}
```

## 6. Bulk 테스트 (REST)

Postman의 **Bulk** 폴더 순서대로 실행.

**테스트 파일 생성**:

```bash
cat > bulk-test.jsonl << 'EOF'
{"customerMessageId": "bulk-001", "recipient": "01012345678", "vars": {"name": "홍길동", "message": "대량 발송 1"}}
{"customerMessageId": "bulk-002", "recipient": "01087654321", "vars": {"name": "김철수", "message": "대량 발송 2"}}
{"customerMessageId": "bulk-003", "recipient": "01011112222", "vars": {"name": "이영희", "message": "대량 발송 3"}}
EOF
gzip -k bulk-test.jsonl
```

1. **Upload** - MinIO에 `bulk-test.jsonl.gz` 업로드
2. **Create Job** - Bulk Job 생성
3. **Get Status** - 상태 조회 (`PENDING` → `PROCESSING` → `PUBLISHED` → `COMPLETED`)

---

## 7. 결과 확인

```bash
# CDR 레코드
docker exec message-receiver-postgres psql -U maiload -d message_receiver \
  -c "SELECT receipt_id, customer_message_id, channel, send_type, status FROM messaging.cdr_records ORDER BY created_at DESC LIMIT 10;"

# Bulk Job 상태
docker exec message-receiver-postgres psql -U maiload -d message_receiver \
  -c "SELECT job_id, status, total_count, success_count, fail_count, skip_count FROM messaging.bulk_jobs ORDER BY created_at DESC LIMIT 5;"
```

---

## 정리

```bash
# 인프라 종료 (데이터 유지)
cd docker && docker-compose down

# 인프라 종료 + 완전 초기화
cd docker && docker-compose down -v && rm -rf data/
```
