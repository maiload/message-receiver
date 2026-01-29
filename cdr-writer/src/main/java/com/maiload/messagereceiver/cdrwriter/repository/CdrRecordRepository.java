package com.maiload.messagereceiver.cdrwriter.repository;

import static com.maiload.messagereceiver.common.domain.MessageStatus.*;
import static com.maiload.messagereceiver.cdrwriter.jooq.tables.CdrRecords.CDR_RECORDS;

import com.maiload.messagereceiver.cdrwriter.service.CdrBatchService.CdrEvent;
import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
@RequiredArgsConstructor
public class CdrRecordRepository {

    private final DSLContext dsl;

    public int batchInsert(List<CdrEvent> events) {
        if (events.isEmpty()) {
            return 0;
        }

        var insert = dsl.insertInto(CDR_RECORDS,
                CDR_RECORDS.CUSTOMER_ID,
                CDR_RECORDS.RECEIPT_ID,
                CDR_RECORDS.CUSTOMER_MESSAGE_ID,
                CDR_RECORDS.SEND_TYPE,
                CDR_RECORDS.CHANNEL,
                CDR_RECORDS.STATUS,
                CDR_RECORDS.PROVIDER_MESSAGE_ID,
                CDR_RECORDS.RECIPIENT_HASH,
                CDR_RECORDS.SEGMENTS,
                CDR_RECORDS.PRICE,
                CDR_RECORDS.FAIL_CODE,
                CDR_RECORDS.FAIL_REASON,
                CDR_RECORDS.ACCEPTED_AT,
                CDR_RECORDS.SENT_AT);

        for (CdrEvent event : events) {
            insert.values(
                    event.customerId(),
                    event.receiptId(),
                    event.customerMessageId(),
                    event.sendType(),
                    event.channel(),
                    event.status(),
                    event.providerMessageId(),
                    event.recipientHash(),
                    event.segments(),
                    event.price(),
                    event.failCode(),
                    event.failReason(),
                    event.occurredAt(),
                    SENT == event.status() ? event.occurredAt() : null);
        }

        return insert
                .onConflict(CDR_RECORDS.CUSTOMER_ID, CDR_RECORDS.CUSTOMER_MESSAGE_ID)
                .doNothing()
                .execute();
    }
}
