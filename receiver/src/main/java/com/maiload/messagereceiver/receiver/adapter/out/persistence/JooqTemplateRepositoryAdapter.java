package com.maiload.messagereceiver.receiver.adapter.out.persistence;

import static com.maiload.messagereceiver.receiver.jooq.tables.Templates.TEMPLATES;

import com.maiload.messagereceiver.receiver.application.port.out.TemplateRepositoryPort;
import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class JooqTemplateRepositoryAdapter implements TemplateRepositoryPort {

    private final DSLContext dsl;

    @Override
    public boolean existsByTemplateIdAndCustomerId(String templateId, String customerId) {
        return dsl.fetchExists(
                dsl.selectOne()
                        .from(TEMPLATES)
                        .where(TEMPLATES.TEMPLATE_ID.eq(templateId)
                                .and(TEMPLATES.CUSTOMER_ID.eq(customerId))));
    }
}
