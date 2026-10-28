package com.example.transactions.domain;

import java.util.List;
import java.util.Map;

public record ProcessingReport(
        long totalSubmissions,
        long duplicatesDetected,
        long conflictsDetected,
        long invalidRecords,
        long retryAttemptsScheduled,
        long processedLedgerEntries,
        long nextExpectedSequence,
        Long firstMissingSequence,
        List<Long> waitingSequences,
        Map<String, Long> countsByCurrentStatus,
        Map<String, String> balancesByAccountCurrency
) {}
