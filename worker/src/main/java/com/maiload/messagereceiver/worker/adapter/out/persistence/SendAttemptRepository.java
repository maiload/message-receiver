package com.maiload.messagereceiver.worker.adapter.out.persistence;

import static com.maiload.messagereceiver.worker.jooq.tables.SendAttempts.SEND_ATTEMPTS;

import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class SendAttemptRepository {

    private final DSLContext dsl;

    public boolean tryLock(String customerId, String customerMessageId, String receiptId, String jobId) {
        return dsl.insertInto(SEND_ATTEMPTS)
                .columns(SEND_ATTEMPTS.CUSTOMER_ID, SEND_ATTEMPTS.CUSTOMER_MESSAGE_ID,
                        SEND_ATTEMPTS.RECEIPT_ID, SEND_ATTEMPTS.JOB_ID, SEND_ATTEMPTS.STATUS)
                .values(customerId, customerMessageId, receiptId, jobId, "LOCKED")
                .onConflict(SEND_ATTEMPTS.CUSTOMER_ID, SEND_ATTEMPTS.CUSTOMER_MESSAGE_ID)
                .doNothing()
                .execute() == 1;
    }

    public void updateStatus(String customerId, String customerMessageId, String status) {
        dsl.update(SEND_ATTEMPTS)
                .set(SEND_ATTEMPTS.STATUS, status)
                .where(SEND_ATTEMPTS.CUSTOMER_ID.eq(customerId)
                        .and(SEND_ATTEMPTS.CUSTOMER_MESSAGE_ID.eq(customerMessageId)))
                .execute();
    }
}
