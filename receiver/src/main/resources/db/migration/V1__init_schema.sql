-- 스키마 생성
CREATE SCHEMA IF NOT EXISTS messaging;

-- customers 테이블
CREATE TABLE messaging.customers (
    id                  BIGSERIAL PRIMARY KEY,
    customer_id         VARCHAR(64) NOT NULL UNIQUE,
    name                VARCHAR(128) NOT NULL,
    api_key_hash        VARCHAR(256) NOT NULL,
    api_key_expires_at  TIMESTAMP,

    -- Rate Limit
    rate_limit_rps      INT NOT NULL DEFAULT 100,

    status              VARCHAR(16) NOT NULL DEFAULT 'ACTIVE',
    created_at          TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_customers_api_key_hash ON messaging.customers(api_key_hash);

-- templates 테이블
CREATE TABLE messaging.templates (
    id                  BIGSERIAL PRIMARY KEY,
    template_id         VARCHAR(64) NOT NULL UNIQUE,
    customer_id         VARCHAR(64) NOT NULL REFERENCES messaging.customers(customer_id),
    channel             VARCHAR(16) NOT NULL,
    name                VARCHAR(128) NOT NULL,
    content             TEXT NOT NULL,

    status              VARCHAR(16) NOT NULL DEFAULT 'ACTIVE',
    created_at          TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_templates_customer ON messaging.templates(customer_id);

-- cdr_records 테이블
CREATE TABLE messaging.cdr_records (
    id                    BIGSERIAL PRIMARY KEY,
    customer_id           VARCHAR(64) NOT NULL,
    receipt_id            VARCHAR(64) NOT NULL,
    customer_message_id   VARCHAR(128) NOT NULL,
    channel               VARCHAR(16) NOT NULL,
    send_type             VARCHAR(16) NOT NULL DEFAULT 'REALTIME',
    status                VARCHAR(32) NOT NULL,
    provider_message_id   VARCHAR(128),
    recipient_hash        VARCHAR(64) NOT NULL,
    fail_code             VARCHAR(32),
    fail_reason           VARCHAR(256),
    accepted_at           TIMESTAMP NOT NULL,
    sent_at               TIMESTAMP,
    created_at            TIMESTAMP NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_cdr_idempotency UNIQUE (customer_id, customer_message_id)
);

CREATE INDEX idx_cdr_records_customer_created ON messaging.cdr_records(customer_id, created_at DESC);
CREATE INDEX idx_cdr_records_receipt ON messaging.cdr_records(receipt_id);

-- bulk_jobs 테이블
CREATE TABLE messaging.bulk_jobs (
    id                    BIGSERIAL PRIMARY KEY,
    job_id                VARCHAR(64) NOT NULL UNIQUE,
    customer_id           VARCHAR(64) NOT NULL,
    template_id           VARCHAR(64),
    object_key            VARCHAR(512) NOT NULL,
    status                VARCHAR(32) NOT NULL,
    total_count           INT NOT NULL DEFAULT 0,
    success_count         INT NOT NULL DEFAULT 0,
    fail_count            INT NOT NULL DEFAULT 0,
    published_chunks      INT NOT NULL DEFAULT 0,
    retry_count           INT NOT NULL DEFAULT 0,
    scheduled_at          TIMESTAMP,
    locked_by             VARCHAR(64),
    locked_until          TIMESTAMP,
    created_at            TIMESTAMP NOT NULL DEFAULT NOW(),
    started_at            TIMESTAMP,
    completed_at          TIMESTAMP
);

CREATE INDEX idx_bulk_jobs_customer ON messaging.bulk_jobs(customer_id, created_at DESC);
CREATE INDEX idx_bulk_jobs_status ON messaging.bulk_jobs(status) WHERE status IN ('PENDING', 'FAILED', 'PROCESSING');

-- send_attempts 테이블 (벌크 발송 중복 방지)
CREATE TABLE messaging.send_attempts (
    id                    BIGSERIAL PRIMARY KEY,
    customer_id           VARCHAR(64) NOT NULL,
    customer_message_id   VARCHAR(128) NOT NULL,
    receipt_id            VARCHAR(64) NOT NULL,
    job_id                VARCHAR(64) NOT NULL,
    status                VARCHAR(16) NOT NULL DEFAULT 'LOCKED',
    created_at            TIMESTAMP NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_send_attempt UNIQUE (customer_id, customer_message_id)
);

CREATE INDEX idx_send_attempts_job ON messaging.send_attempts(job_id);

-----------------------------------------------------------
-- 시드 데이터 (테스트용)
-----------------------------------------------------------

-- 테스트 고객 (API Key: test-api-key)
INSERT INTO messaging.customers (customer_id, name, api_key_hash, rate_limit_rps)
VALUES (
    'cust-001',
    'Test Customer',
    '4c806362b613f7496abf284146efd31da90e4b16169fe001841ca17290f427c4',
    100
) ON CONFLICT (customer_id) DO NOTHING;

-- SMS 템플릿 (변수 포함)
INSERT INTO messaging.templates (template_id, customer_id, channel, name, content)
VALUES (
    'tpl-hello',
    'cust-001',
    'SMS',
    '인사 템플릿',
    '안녕하세요 {{name}}님, {{message}}'
) ON CONFLICT (template_id) DO NOTHING;

-- SMS 템플릿 (단순)
INSERT INTO messaging.templates (template_id, customer_id, channel, name, content)
VALUES (
    'tpl-simple',
    'cust-001',
    'SMS',
    '단순 알림',
    '서비스 점검 안내입니다.'
) ON CONFLICT (template_id) DO NOTHING;
