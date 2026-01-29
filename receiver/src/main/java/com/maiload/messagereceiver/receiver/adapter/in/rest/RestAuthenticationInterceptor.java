package com.maiload.messagereceiver.receiver.adapter.in.rest;

import com.maiload.messagereceiver.common.exception.AuthenticationException;
import com.maiload.messagereceiver.common.exception.ErrorCode;
import com.maiload.messagereceiver.receiver.application.port.out.CustomerRepositoryPort;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.servlet.HandlerInterceptor;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDateTime;
import java.util.HexFormat;

@Component
@RequiredArgsConstructor
public class RestAuthenticationInterceptor implements HandlerInterceptor {

    public static final String CUSTOMER_ID_ATTR = "customerId";

    private final CustomerRepositoryPort customerRepositoryPort;

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) {
        String apiKey = request.getHeader("x-api-key");
        if (!StringUtils.hasText(apiKey)) {
            throw new AuthenticationException(ErrorCode.UNAUTHENTICATED, "API key is required");
        }

        String apiKeyHash = hashApiKey(apiKey);
        var customer = customerRepositoryPort.findByApiKeyHash(apiKeyHash)
                .orElseThrow(() -> new AuthenticationException(ErrorCode.UNAUTHENTICATED, "Invalid API key"));

        if (!"ACTIVE".equals(customer.status())) {
            throw new AuthenticationException(ErrorCode.UNAUTHENTICATED, "Customer account is not active");
        }

        if (customer.apiKeyExpiresAt() != null && customer.apiKeyExpiresAt().isBefore(LocalDateTime.now())) {
            throw new AuthenticationException(ErrorCode.API_KEY_EXPIRED, "API key has expired");
        }

        request.setAttribute(CUSTOMER_ID_ATTR, customer.customerId());
        return true;
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
