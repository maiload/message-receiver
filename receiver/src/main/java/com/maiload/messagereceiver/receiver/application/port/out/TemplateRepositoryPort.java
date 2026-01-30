package com.maiload.messagereceiver.receiver.application.port.out;

public interface TemplateRepositoryPort {

    boolean existsByTemplateIdAndCustomerId(String templateId, String customerId);
}
