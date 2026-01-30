package com.maiload.messagereceiver.receiver.adapter.in.grpc;

import com.google.protobuf.Timestamp;
import com.maiload.messagereceiver.common.exception.BaseException;
import com.maiload.messagereceiver.common.exception.ErrorCode;
import com.maiload.messagereceiver.grpc.GetReceiptStatusRequest;
import com.maiload.messagereceiver.grpc.GetReceiptStatusResponse;
import com.maiload.messagereceiver.common.domain.MessageStatus;
import com.maiload.messagereceiver.grpc.DeliveryStatus;
import com.maiload.messagereceiver.grpc.RealtimeMessageServiceGrpc;
import com.maiload.messagereceiver.grpc.SubmitRequest;
import com.maiload.messagereceiver.grpc.SubmitResponse;
import com.maiload.messagereceiver.receiver.application.port.in.RealtimeMessagePort;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class GrpcRealtimeMessageAdapter extends RealtimeMessageServiceGrpc.RealtimeMessageServiceImplBase {

    private final RealtimeMessagePort realtimeMessagePort;

    @Override
    public void submit(SubmitRequest request, StreamObserver<SubmitResponse> responseObserver) {
        try {
            if (request.getCustomerId().isEmpty()) {
                responseObserver.onError(Status.INVALID_ARGUMENT
                        .withDescription("customer_id is required")
                        .asRuntimeException());
                return;
            }

            String authenticatedCustomerId = AuthenticationInterceptor.CUSTOMER_ID_CTX_KEY.get();
            if (!request.getCustomerId().equals(authenticatedCustomerId)) {
                responseObserver.onError(Status.PERMISSION_DENIED
                        .withDescription("Customer ID mismatch")
                        .asRuntimeException());
                return;
            }

            RealtimeMessagePort.SubmitResult result = realtimeMessagePort.submit(toSubmit(request));
            responseObserver.onNext(toResponse(result));
            responseObserver.onCompleted();
        } catch (BaseException e) {
            log.warn("Business exception: code={}, message={}", e.getErrorCode().getCode(), e.getMessage());
            responseObserver.onError(toGrpcStatus(e.getErrorCode()).withDescription(e.getMessage()).asRuntimeException());
        } catch (Exception e) {
            log.error("Unexpected exception", e);
            responseObserver.onError(Status.INTERNAL.withDescription("Internal server error").asRuntimeException());
        }
    }

    @Override
    public void getReceiptStatus(GetReceiptStatusRequest request, StreamObserver<GetReceiptStatusResponse> responseObserver) {
        try {
            String authenticatedCustomerId = AuthenticationInterceptor.CUSTOMER_ID_CTX_KEY.get();
            if (!request.getCustomerId().equals(authenticatedCustomerId)) {
                responseObserver.onError(Status.PERMISSION_DENIED
                        .withDescription("Customer ID mismatch")
                        .asRuntimeException());
                return;
            }

            RealtimeMessagePort.ReceiptStatus result = realtimeMessagePort.getReceiptStatus(
                    request.getCustomerId(), request.getReceiptId());

            responseObserver.onNext(toReceiptStatusResponse(result));
            responseObserver.onCompleted();
        } catch (BaseException e) {
            log.warn("Business exception: code={}, message={}", e.getErrorCode().getCode(), e.getMessage());
            responseObserver.onError(toGrpcStatus(e.getErrorCode()).withDescription(e.getMessage()).asRuntimeException());
        } catch (Exception e) {
            log.error("Unexpected exception", e);
            responseObserver.onError(Status.INTERNAL.withDescription("Internal server error").asRuntimeException());
        }
    }

    private RealtimeMessagePort.Submit toSubmit(SubmitRequest request) {
        return new RealtimeMessagePort.Submit(
                request.getCustomerId(),
                request.getCustomerMessageId(),
                request.getMessageType().name(),
                request.getRecipient(),
                request.getTemplateId(),
                request.getContent(),
                Map.copyOf(request.getVarsMap()),
                request.getTtlSeconds() > 0 ? request.getTtlSeconds() : null,
                List.copyOf(request.getMediaUrlsList())
        );
    }

    private SubmitResponse toResponse(RealtimeMessagePort.SubmitResult result) {
        return SubmitResponse.newBuilder()
                .setReceiptId(result.receiptId())
                .setAcceptedAt(toTimestamp(result.acceptedAt()))
                .setIdempotencyHit(result.idempotencyHit())
                .build();
    }

    private GetReceiptStatusResponse toReceiptStatusResponse(RealtimeMessagePort.ReceiptStatus result) {
        var builder = GetReceiptStatusResponse.newBuilder();
        builder.setReceiptId(result.receiptId())
                .setCustomerMessageId(result.customerMessageId())
                .setStatus(toDeliveryStatus(result.status()))
                .setAcceptedAt(toTimestamp(result.acceptedAt()));

        if (result.failCode() != null) builder.setFailCode(result.failCode());
        if (result.failReason() != null) builder.setFailReason(result.failReason());
        if (result.sentAt() != null) builder.setSentAt(toTimestamp(result.sentAt()));

        return builder.build();
    }

    private DeliveryStatus toDeliveryStatus(MessageStatus status) {
        if (status == null) return DeliveryStatus.DELIVERY_STATUS_UNSPECIFIED;
        return switch (status) {
            case SENT -> DeliveryStatus.SENT;
            case FAILED -> DeliveryStatus.FAILED;
            case SKIPPED -> DeliveryStatus.DELIVERY_STATUS_UNSPECIFIED;
        };
    }

    private Timestamp toTimestamp(LocalDateTime localDateTime) {
        var instant = localDateTime.atZone(ZoneId.systemDefault()).toInstant();
        return Timestamp.newBuilder()
                .setSeconds(instant.getEpochSecond())
                .setNanos(instant.getNano())
                .build();
    }

    private Status toGrpcStatus(ErrorCode errorCode) {
        return switch (errorCode) {
            case UNAUTHENTICATED, API_KEY_EXPIRED ->
                    Status.UNAUTHENTICATED;
            case PERMISSION_DENIED ->
                    Status.PERMISSION_DENIED;
            case INVALID_REQUEST, INVALID_RECIPIENT, INVALID_TEMPLATE ->
                    Status.INVALID_ARGUMENT;
            case TEMPLATE_NOT_FOUND, RECEIPT_NOT_FOUND ->
                    Status.NOT_FOUND;
            case RATE_LIMIT_EXCEEDED ->
                    Status.RESOURCE_EXHAUSTED;
            case IDEMPOTENCY_CONFLICT ->
                    Status.ALREADY_EXISTS;
            case REDIS_CONNECTION_FAILED, MQ_PUBLISH_FAILED, KAFKA_PUBLISH_FAILED, DB_CONNECTION_FAILED,
                 GATEWAY_TIMEOUT, GATEWAY_5XX ->
                    Status.UNAVAILABLE;
            default ->
                    Status.INTERNAL;
        };
    }
}
