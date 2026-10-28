package com.example.transactions.domain;

import java.math.BigDecimal;
import java.time.Instant;

public record LedgerEntry(
        String transactionId,
        String accountId,
        long sequenceNumber,
        String transactionType,
        BigDecimal amount,
        String currency,
        BigDecimal resultingBalance,
        Instant processedAt
) {}
