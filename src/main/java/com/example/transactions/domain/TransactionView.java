package com.example.transactions.domain;

public record TransactionView(
        String transactionId,
        String requestId,
        Long sequenceNumber,
        String accountId,
        String transactionType,
        String amount,
        String currency,
        TransactionStatus status,
        int attempts,
        String lastError
) {}
