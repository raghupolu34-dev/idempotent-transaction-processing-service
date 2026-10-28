package com.example.transactions.domain;

import java.math.BigDecimal;

/** A request from an untrusted input adapter; validation happens in the engine. */
public record TransactionRequest(
        String transactionId,
        String requestId,
        Long sequenceNumber,
        String accountId,
        String transactionType,
        BigDecimal amount,
        String currency,
        Integer simulateTransientFailures
) {}
