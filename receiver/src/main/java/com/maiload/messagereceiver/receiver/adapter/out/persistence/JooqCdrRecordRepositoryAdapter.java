package com.maiload.messagereceiver.receiver.adapter.out.persistence;

import com.maiload.messagereceiver.receiver.application.port.out.CdrRecordRepositoryPort;
import com.maiload.messagereceiver.receiver.jooq.tables.CdrRecords;
import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
@RequiredArgsConstructor
public class JooqCdrRecordRepositoryAdapter implements CdrRecordRepositoryPort {

    private static final CdrRecords CDR_RECORDS = CdrRecords.CDR_RECORDS;

    private final DSLContext dsl;

    @Override
    public Optional<CdrRecord> findByCustomerIdAndReceiptId(String customerId, String receiptId) {
        return dsl.select(
                        CDR_RECORDS.RECEIPT_ID,
                        CDR_RECORDS.CUSTOMER_MESSAGE_ID,
                        CDR_RECORDS.STATUS,
                        CDR_RECORDS.FAIL_CODE,
                        CDR_RECORDS.FAIL_REASON,
                        CDR_RECORDS.ACCEPTED_AT,
                        CDR_RECORDS.SENT_AT)
                .from(CDR_RECORDS)
                .where(CDR_RECORDS.CUSTOMER_ID.eq(customerId)
                        .and(CDR_RECORDS.RECEIPT_ID.eq(receiptId)))
                .fetchOptionalInto(CdrRecord.class);
    }
}
