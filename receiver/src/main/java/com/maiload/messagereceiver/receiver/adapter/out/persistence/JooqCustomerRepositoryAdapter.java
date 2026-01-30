package com.maiload.messagereceiver.receiver.adapter.out.persistence;

import com.maiload.messagereceiver.receiver.application.port.out.CustomerRepositoryPort;
import com.maiload.messagereceiver.receiver.jooq.tables.Customers;
import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
@RequiredArgsConstructor
public class JooqCustomerRepositoryAdapter implements CustomerRepositoryPort {

    private static final Customers CUSTOMERS = Customers.CUSTOMERS;

    private final DSLContext dsl;

    @Override
    public Optional<Customer> findByApiKeyHash(String apiKeyHash) {
        return dsl.select(
                        CUSTOMERS.CUSTOMER_ID,
                        CUSTOMERS.NAME,
                        CUSTOMERS.API_KEY_EXPIRES_AT,
                        CUSTOMERS.RATE_LIMIT_RPS,
                        CUSTOMERS.STATUS)
                .from(CUSTOMERS)
                .where(CUSTOMERS.API_KEY_HASH.eq(apiKeyHash))
                .fetchOptionalInto(Customer.class);
    }
}
