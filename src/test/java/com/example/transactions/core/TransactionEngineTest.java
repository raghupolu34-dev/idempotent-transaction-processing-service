package com.example.transactions.core;

import com.example.transactions.domain.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

class TransactionEngineTest {
    TransactionEngine engine;

    @BeforeEach
    void setup() {
        engine = new TransactionEngine(Clock.fixed(Instant.parse("2026-01-01T00:00:00Z"),
                ZoneOffset.UTC), 3, Duration.ZERO);
    }

    static TransactionRequest req(String txn, String reqId, long seq, String acc, String type,
                                  String amt, String ccy, int failCount) {
        return new TransactionRequest(txn, reqId, seq, acc, type,
                new BigDecimal(amt), ccy, failCount);
    }

    @Test
    void outOfOrderIsBufferedAndThenProcessedInSequence() {
        assertEquals(TransactionStatus.PENDING_ORDER,
                engine.submit(req("TX2", "REQ2", 2, "A", "DEBIT", "40", "INR", 0)).status());
        assertEquals(0, engine.processReady(100));
        assertEquals(1L, engine.getReport().firstMissingSequence());
        engine.submit(req("TX1", "REQ1", 1, "A", "CREDIT", "100", "INR", 0));
        assertEquals(2, engine.processReady(100));
        assertEquals(0, new BigDecimal("60").compareTo(engine.getBalances("A").get("INR")));
        assertEquals(2, engine.getLedger().size());
        assertEquals(3, engine.getReport().nextExpectedSequence());
    }

    @Test
    void sameTransactionDifferentRequestIdCreatesOnlyOneEffect() {
        engine.submit(req("TX1", "REQ1", 1, "A", "CREDIT", "100", "INR", 0));
        engine.processReady(10);
        assertEquals(TransactionStatus.DUPLICATE,
                engine.submit(req("TX1", "REQ-RETRY", 1, "A", "CREDIT", "100.00", "INR", 0)).status());
        assertEquals(0, engine.processReady(10));
        assertEquals(1, engine.getLedger().size());
        assertEquals(1, engine.getReport().duplicatesDetected());
    }

    @Test
    void duplicateWhileStillPendingDoesNotReserveAnotherSequence() {
        engine.submit(req("TX2", "REQ2", 2, "A", "DEBIT", "40", "INR", 0));
        assertEquals(TransactionStatus.DUPLICATE,
                engine.submit(req("TX2", "REQ2B", 2, "A", "DEBIT", "40", "INR", 0)).status());
        engine.submit(req("TX1", "REQ1", 1, "A", "CREDIT", "100", "INR", 0));
        engine.processReady(10);
        assertEquals(2, engine.getLedger().size());
    }

    @Test
    void differentPayloadForSameTransactionIsConflict() {
        engine.submit(req("TX1", "REQ1", 1, "A", "CREDIT", "100", "INR", 0));
        assertEquals(409, engine.submit(req("TX1", "REQ2", 1, "A", "CREDIT", "999", "INR", 0)).httpStatus());
        engine.processReady(10);
        assertEquals(0, new BigDecimal("100").compareTo(engine.getBalances("A").get("INR")));
    }

    @Test
    void reusedRequestIdForAnotherTransactionIsConflict() {
        engine.submit(req("TX1", "SAME", 1, "A", "CREDIT", "100", "INR", 0));
        assertEquals(TransactionStatus.CONFLICT,
                engine.submit(req("TX2", "SAME", 2, "A", "CREDIT", "999", "INR", 0)).status());
    }

    @Test
    void malformedSequencedRecordIsAuditedAndDoesNotStopBatch() {
        engine.submit(req("TX1", "REQ1", 1, "A", "CREDIT", "100", "INR", 0));
        var invalid = engine.submit(req("TX2", "REQ2", 2, "A", "DEBIT", "-10", "INR", 0));
        assertEquals(422, invalid.httpStatus());
        engine.submit(req("TX3", "REQ3", 3, "A", "CREDIT", "25", "INR", 0));
        engine.processReady(100);
        assertEquals(2, engine.getLedger().size());
        assertEquals(TransactionStatus.FAILED, engine.getTransaction("TX2").status());
        assertEquals(0, new BigDecimal("125").compareTo(engine.getBalances("A").get("INR")));
        assertEquals(4, engine.getReport().nextExpectedSequence());
    }

    @Test
    void retryOnceThenProcessExactlyOnce() {
        engine.submit(req("TX1", "REQ1", 1, "A", "CREDIT", "500", "INR", 1));
        engine.processReady(10);
        assertEquals(TransactionStatus.PROCESSED, engine.getTransaction("TX1").status());
        assertEquals(2, engine.getTransaction("TX1").attempts());
        assertEquals(1, engine.getReport().retryAttemptsScheduled());
        assertEquals(1, engine.getLedger().size());
    }

    @Test
    void exhaustedRetryIsFailedAndNextTransactionCanRun() {
        engine.submit(req("TX1", "REQ1", 1, "A", "CREDIT", "500", "INR", 5));
        engine.submit(req("TX2", "REQ2", 2, "A", "CREDIT", "10", "INR", 0));
        engine.processReady(20);
        assertEquals(TransactionStatus.FAILED, engine.getTransaction("TX1").status());
        assertEquals(3, engine.getTransaction("TX1").attempts());
        assertEquals(TransactionStatus.PROCESSED, engine.getTransaction("TX2").status());
        assertEquals(1, engine.getLedger().size());
        assertEquals(0, new BigDecimal("10").compareTo(engine.getBalances("A").get("INR")));
    }

    @Test
    void invalidIdentityDoesNotCorruptAnyLedgerState() {
        var invalid = engine.submit(new TransactionRequest("TX1", "REQ1", null,
                "A", "CREDIT", new BigDecimal("100"), "INR", 0));
        assertEquals(TransactionStatus.REJECTED, invalid.status());
        assertEquals(0, engine.getLedger().size());
    }

    @Test
    void accountBalancesAreSeparatedByCurrency() {
        engine.submit(req("TX1", "REQ1", 1, "A", "CREDIT", "100", "INR", 0));
        engine.submit(req("TX2", "REQ2", 2, "A", "CREDIT", "20", "USD", 0));
        engine.processReady(10);
        assertEquals(2, engine.getBalances("A").size());
    }

    @Test
    void simultaneousDuplicateSubmissionsRemainExactlyOnceInOneJvm() throws Exception {
        var request = req("TX1", "REQ1", 1, "A", "CREDIT", "100", "INR", 0);
        ExecutorService pool = Executors.newFixedThreadPool(8);
        try {
            var tasks = java.util.stream.IntStream.range(0, 20)
                    .mapToObj(i -> (Callable<SubmissionResponse>) () -> engine.submit(request)).toList();
            var results = pool.invokeAll(tasks);
            long accepted = 0;
            for (var future : results) if (future.get().status() == TransactionStatus.RECEIVED) accepted++;
            assertEquals(1, accepted);
            engine.processReady(10);
            assertEquals(1, engine.getLedger().size());
            assertEquals(19, engine.getReport().duplicatesDetected());
        } finally {
            pool.shutdownNow();
        }
    }
}
