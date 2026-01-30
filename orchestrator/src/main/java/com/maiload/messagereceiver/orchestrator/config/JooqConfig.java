package com.maiload.messagereceiver.orchestrator.config;

import org.jooq.conf.Settings;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class JooqConfig {

    @Bean
    public Settings jooqSettings() {
        return new Settings()
                .withMapRecordComponentParameterNames(true);
    }
}
