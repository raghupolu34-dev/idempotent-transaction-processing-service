package com.example.transactions.domain;

public enum TransactionStatus {
    RECEIVED,
    PENDING_ORDER,
    PROCESSING,
    RETRY_PENDING,
    PROCESSED,
    DUPLICATE,     // Used for duplicate submission acknowledgements only.
    FAILED,        // Terminal business processing or validation failure.
    REJECTED,      // Submission had no usable sequence/identity.
    CONFLICT       // Different payload for same transaction/request/sequence identity.
}
