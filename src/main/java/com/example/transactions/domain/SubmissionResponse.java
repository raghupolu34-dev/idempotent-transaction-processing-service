package com.example.transactions.domain;

/** httpStatus is included so a batch can show each record's own result. */
public record SubmissionResponse(
        String transactionId,
        String requestId,
        Long sequenceNumber,
        TransactionStatus status,
        String message,
        int httpStatus
) {}
