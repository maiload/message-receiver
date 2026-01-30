package com.maiload.messagereceiver.orchestrator.repository;

import static com.maiload.messagereceiver.orchestrator.jooq.tables.Templates.TEMPLATES;

import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
@RequiredArgsConstructor
public class TemplateRepository {

    private final DSLContext dsl;

    public Optional<TemplateInfo> findByTemplateId(String templateId) {
        return dsl.select(TEMPLATES.CHANNEL, TEMPLATES.CONTENT)
                .from(TEMPLATES)
                .where(TEMPLATES.TEMPLATE_ID.eq(templateId))
                .fetchOptionalInto(TemplateInfo.class);
    }

    public record TemplateInfo(String channel, String content) {}
}
