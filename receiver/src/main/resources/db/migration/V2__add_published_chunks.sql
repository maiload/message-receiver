ALTER TABLE messaging.bulk_jobs
    ADD COLUMN published_chunks INT NOT NULL DEFAULT 0,
    ADD COLUMN retry_count INT NOT NULL DEFAULT 0;
