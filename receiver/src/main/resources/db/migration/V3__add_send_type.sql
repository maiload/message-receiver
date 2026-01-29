ALTER TABLE messaging.cdr_records
    ADD COLUMN send_type VARCHAR(16) NOT NULL DEFAULT 'REALTIME';
