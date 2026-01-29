package com.maiload.messagereceiver.receiver.adapter.in.grpc;

import com.google.protobuf.Timestamp;
import com.maiload.messagereceiver.common.exception.BaseException;
import com.maiload.messagereceiver.common.exception.ErrorCode;
import com.maiload.messagereceiver.grpc.GetReceiptStatusRequest;
import com.maiload.messagereceiver.grpc.GetReceiptStatusResponse;
import com.maiload.messagereceiver.grpc.RealtimeMessageServiceGrpc;
import com.maiload.messagereceiver.grpc.SubmitRequest;
import com.maiload.messagereceiver.grpc.SubmitResponse;
import com.maiload.messagereceiver.receiver.application.port.in.RealtimeMessagePort;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Instant;
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
        // TODO: CDR Writer가 DB에 상태 저장 후 구현 (Phase 3 이후)
        responseObserver.onError(Status.UNIMPLEMENTED.asRuntimeException());
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

    private Timestamp toTimestamp(Instant instant) {
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
            case TEMPLATE_NOT_FOUND ->
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
