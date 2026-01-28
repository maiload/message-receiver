-- 테넌트 (고객사)
CREATE TABLE messaging.tenant (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name            VARCHAR(100) NOT NULL,
    api_key         VARCHAR(64) NOT NULL UNIQUE,
    daily_quota     BIGINT NOT NULL DEFAULT 100000,
    rate_limit      INT NOT NULL DEFAULT 100,
    active          BOOLEAN NOT NULL DEFAULT true,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_tenant_api_key ON messaging.tenant(api_key);

-- 발신번호
CREATE TABLE messaging.sender (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id       UUID NOT NULL REFERENCES messaging.tenant(id),
    phone_number    VARCHAR(20) NOT NULL,
    channel_type    VARCHAR(20) NOT NULL,
    verified        BOOLEAN NOT NULL DEFAULT false,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE(tenant_id, phone_number, channel_type)
);

CREATE INDEX idx_sender_tenant ON messaging.sender(tenant_id);

-- 메시지 발송 기록 (CDR)
CREATE TABLE messaging.message (
    id              UUID PRIMARY KEY,
    tenant_id       UUID NOT NULL,
    client_msg_id   VARCHAR(100),
    channel_type    VARCHAR(20) NOT NULL,
    send_type       VARCHAR(20) NOT NULL,
    sender          VARCHAR(20) NOT NULL,
    recipient       VARCHAR(20) NOT NULL,
    subject         VARCHAR(100),
    content         TEXT NOT NULL,
    status          VARCHAR(20) NOT NULL,
    job_id          UUID,
    gateway_msg_id  VARCHAR(100),
    sent_at         TIMESTAMPTZ,
    delivered_at    TIMESTAMPTZ,
    failed_reason   TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_message_tenant_created ON messaging.message(tenant_id, created_at DESC);
CREATE INDEX idx_message_client_msg_id ON messaging.message(tenant_id, client_msg_id);
CREATE INDEX idx_message_job ON messaging.message(job_id) WHERE job_id IS NOT NULL;
CREATE INDEX idx_message_status ON messaging.message(status) WHERE status NOT IN ('DELIVERED', 'FAILED', 'EXPIRED');

-- Bulk 작업
CREATE TABLE messaging.bulk_job (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id       UUID NOT NULL REFERENCES messaging.tenant(id),
    name            VARCHAR(200),
    channel_type    VARCHAR(20) NOT NULL,
    file_path       VARCHAR(500) NOT NULL,
    total_count     BIGINT NOT NULL DEFAULT 0,
    success_count   BIGINT NOT NULL DEFAULT 0,
    fail_count      BIGINT NOT NULL DEFAULT 0,
    status          VARCHAR(20) NOT NULL,
    scheduled_at    TIMESTAMPTZ,
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_bulk_job_tenant ON messaging.bulk_job(tenant_id, created_at DESC);
CREATE INDEX idx_bulk_job_status ON messaging.bulk_job(status) WHERE status IN ('PENDING', 'VALIDATING', 'SCHEDULED', 'PROCESSING');
