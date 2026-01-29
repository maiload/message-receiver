package com.maiload.messagereceiver.receiver.adapter.in.grpc;

import com.maiload.messagereceiver.receiver.application.port.out.CustomerRepositoryPort;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.grpc.server.GlobalServerInterceptor;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDateTime;
import java.util.HexFormat;

@Slf4j
@Component
@GlobalServerInterceptor
@RequiredArgsConstructor
public class AuthenticationInterceptor implements ServerInterceptor {

    private static final Metadata.Key<String> API_KEY_HEADER =
            Metadata.Key.of("x-api-key", Metadata.ASCII_STRING_MARSHALLER);

    public static final Context.Key<String> CUSTOMER_ID_CTX_KEY = Context.key("customerId");
    public static final Context.Key<Integer> RATE_LIMIT_RPS_CTX_KEY = Context.key("rateLimitRps");

    private final CustomerRepositoryPort customerRepositoryPort;

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call,
            Metadata headers,
            ServerCallHandler<ReqT, RespT> next) {

        String apiKey = headers.get(API_KEY_HEADER);
        if (!StringUtils.hasText(apiKey)) {
            call.close(Status.UNAUTHENTICATED.withDescription("API key is required"), new Metadata());
            return new ServerCall.Listener<>() {};
        }

        String apiKeyHash = hashApiKey(apiKey);
        var customerOpt = customerRepositoryPort.findByApiKeyHash(apiKeyHash);

        if (customerOpt.isEmpty()) {
            call.close(Status.UNAUTHENTICATED.withDescription("Invalid API key"), new Metadata());
            return new ServerCall.Listener<>() {};
        }

        var customer = customerOpt.get();

        if (!"ACTIVE".equals(customer.status())) {
            call.close(Status.UNAUTHENTICATED.withDescription("Customer account is not active"), new Metadata());
            return new ServerCall.Listener<>() {};
        }

        if (customer.apiKeyExpiresAt() != null && customer.apiKeyExpiresAt().isBefore(LocalDateTime.now())) {
            call.close(Status.UNAUTHENTICATED.withDescription("API key has expired"), new Metadata());
            return new ServerCall.Listener<>() {};
        }

        Context ctx = Context.current()
                .withValue(CUSTOMER_ID_CTX_KEY, customer.customerId())
                .withValue(RATE_LIMIT_RPS_CTX_KEY, customer.rateLimitRps());

        return Contexts.interceptCall(ctx, call, headers, next);
    }

    private String hashApiKey(String apiKey) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(apiKey.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hash);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 algorithm not available", e);
        }
    }
}
