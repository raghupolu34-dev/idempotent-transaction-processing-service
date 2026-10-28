package com.example.transactions.core;

import com.example.transactions.domain.*;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Intentionally small in-memory transaction engine. All public operations are synchronized:
 * within ONE JVM, state transition + balance update + ledger append are atomic.
 * Production deployment must replace this with durable transactional storage (see README).
 *
 * Ordering contract: a SINGLE globally sequenced upstream feed beginning at 1.
 * Invalid events with a usable sequence occupy their slot and are audited/skipped.
 */
public final class TransactionEngine {
    private static final System.Logger LOG = System.getLogger(TransactionEngine.class.getName());
    private final Clock clock;
    private final int maxAttempts;
    private final Duration retryDelay;

    private final NavigableMap<Long, Job> bySequence = new TreeMap<>();
    private final Map<String, Job> byTransaction = new LinkedHashMap<>();
    private final Map<String, String> requestOwner = new HashMap<>();
    private final Map<String, BigDecimal> balances = new TreeMap<>();
    private final List<LedgerEntry> ledger = new ArrayList<>();

    private long nextSequence = 1;
    private long submissions;
    private long duplicates;
    private long conflicts;
    private long invalids;
    private long retries;

    public TransactionEngine(Clock clock, int maxAttempts, Duration retryDelay) {
        this.clock = Objects.requireNonNull(clock);
        if (maxAttempts < 1 || maxAttempts > 20) {
            throw new IllegalArgumentException("maxAttempts must be 1..20");
        }
        if (retryDelay.isNegative()) {
            throw new IllegalArgumentException("retryDelay must not be negative");
        }
        this.maxAttempts = maxAttempts;
        this.retryDelay = Objects.requireNonNull(retryDelay);
    }

    /**
     * Accepts one request without immediately applying its business effect.
     * 202 = accepted, 200 = duplicate, 422 = malformed business input,
     * 400 = no usable business identity/sequence, 409 = conflicting identity.
     */
    public synchronized SubmissionResponse submit(TransactionRequest request) {
        submissions++;
        if (request == null || !safeIdentifier(request.transactionId()) || !safeIdentifier(request.requestId())
                || request.sequenceNumber() == null || request.sequenceNumber() < 1) {
            invalids++;
            return result(request, TransactionStatus.REJECTED,
                    "Valid transactionId and requestId (1-80 letters/digits/._:-) and positive sequenceNumber are required", 400);
        }

        String requestId = request.requestId().trim();
        String transactionId = request.transactionId().trim();
        Job knownTxn = byTransaction.get(transactionId);
        String knownRequestTxn = requestOwner.get(requestId);

        // Do not allow a request ID to be reused for a different transaction.
        if (knownRequestTxn != null && !knownRequestTxn.equals(transactionId)) {
            conflicts++;
            LOG.log(System.Logger.Level.WARNING, "event=conflict transactionId={0} reason=requestId_reuse", transactionId);
            return result(request, TransactionStatus.CONFLICT,
                    "requestId already belongs to a different transaction", 409);
        }
        // transactionId is the BUSINESS idempotency key; requestId may vary on redelivery.
        if (knownTxn != null) {
            if (sameBusinessTransaction(knownTxn.request, request)) {
                duplicates++;
                LOG.log(System.Logger.Level.INFO, "event=duplicate transactionId={0}", transactionId);
                requestOwner.put(requestId, transactionId);
                return result(request, TransactionStatus.DUPLICATE,
                        "Already received; original status=" + knownTxn.status, 200);
            }
            conflicts++;
            LOG.log(System.Logger.Level.WARNING, "event=conflict transactionId={0} reason=changed_payload", transactionId);
            return result(request, TransactionStatus.CONFLICT,
                    "transactionId reused with different business data", 409);
        }
        // Prevent the same feed position from being silently overwritten.
        if (request.sequenceNumber() < nextSequence || bySequence.containsKey(request.sequenceNumber())) {
            conflicts++;
            return result(request, TransactionStatus.CONFLICT,
                    "sequenceNumber already consumed or reserved", 409);
        }

        String problem = validationProblem(request);
        TransactionStatus status = problem == null
                ? (request.sequenceNumber() == nextSequence
                    ? TransactionStatus.RECEIVED : TransactionStatus.PENDING_ORDER)
                : TransactionStatus.FAILED;

        Job job = new Job(request, status);
        if (problem != null) {
            invalids++;
            job.lastError = "Validation: " + problem;
            LOG.log(System.Logger.Level.WARNING, "event=invalid transactionId={0} reason={1}", transactionId, problem);
        }
        bySequence.put(request.sequenceNumber(), job);
        byTransaction.put(transactionId, job);
        requestOwner.put(requestId, transactionId);
        return result(request, status,
                problem == null ? "Queued for background processing" : job.lastError,
                problem == null ? 202 : 422);
    }

    /** Process up to limit available ordered slots / retry attempts; returns work attempted. */
    public synchronized int processReady(int limit) {
        if (limit < 1 || limit > 10000) throw new IllegalArgumentException("limit must be 1..10000");
        int attempts = 0;
        while (attempts < limit) {
            Job current = bySequence.get(nextSequence);
            if (current == null) break; // A missing earlier sequence MUST NOT be silently bypassed.
            if (current.status == TransactionStatus.FAILED) {
                // A malformed, but sequenced, event has a definitive audit record and no side effect.
                LOG.log(System.Logger.Level.INFO, "event=invalid_skipped transactionId={0}", current.request.transactionId());
                nextSequence++;
                attempts++;
                continue;
            }
            if (current.status == TransactionStatus.RETRY_PENDING
                    && clock.instant().isBefore(current.nextAttempt)) break;

            attempts++;
            current.status = TransactionStatus.PROCESSING;
            current.attempts++;

            int failures = current.request.simulateTransientFailures() == null
                    ? 0 : current.request.simulateTransientFailures();
            if (current.attempts <= failures) {
                current.lastError = "Simulated transient upstream failure";
                if (current.attempts >= maxAttempts) {
                    current.status = TransactionStatus.FAILED;
                    LOG.log(System.Logger.Level.WARNING, "event=retry_exhausted transactionId={0} attempts={1}",
                            current.request.transactionId(), current.attempts);
                    nextSequence++; // Terminal failure: audited and does not block later slots.
                } else {
                    current.status = TransactionStatus.RETRY_PENDING;
                    retries++;
                    // 1x, 2x, 4x... exponential backoff capped to safe maxAttempts.
                    long multiplier = 1L << Math.min(16, current.attempts - 1);
                    current.nextAttempt = clock.instant().plus(retryDelay.multipliedBy(multiplier));
                    LOG.log(System.Logger.Level.WARNING, "event=retry_scheduled transactionId={0} attempt={1}",
                            current.request.transactionId(), current.attempts);
                }
                continue;
            }

            // Single in-JVM critical section contains the entire financial business update.
            String accountCurrency = accountCurrencyKey(current.request.accountId(), current.request.currency());
            BigDecimal delta = current.request.transactionType().trim().equalsIgnoreCase("CREDIT")
                    ? current.request.amount() : current.request.amount().negate();
            BigDecimal updated = balances.getOrDefault(accountCurrency, BigDecimal.ZERO).add(delta);
            balances.put(accountCurrency, updated);
            ledger.add(new LedgerEntry(
                    current.request.transactionId().trim(), current.request.accountId().trim(),
                    current.request.sequenceNumber(), current.request.transactionType().trim().toUpperCase(Locale.ROOT),
                    current.request.amount(), current.request.currency().trim().toUpperCase(Locale.ROOT),
                    updated, clock.instant()));
            current.status = TransactionStatus.PROCESSED;
            current.lastError = null;
            LOG.log(System.Logger.Level.INFO, "event=processed transactionId={0} sequence={1}",
                    current.request.transactionId(), current.request.sequenceNumber());
            nextSequence++;
        }
        return attempts;
    }

    public synchronized TransactionView getTransaction(String transactionId) {
        Job job = byTransaction.get(transactionId);
        if (job == null) return null;
        TransactionRequest req = job.request;
        return new TransactionView(req.transactionId(), req.requestId(), req.sequenceNumber(),
                req.accountId(), req.transactionType(), req.amount() == null ? null : req.amount().toPlainString(),
                req.currency(), job.status, job.attempts, job.lastError);
    }

    public synchronized List<LedgerEntry> getLedger() {
        return List.copyOf(ledger);
    }

    public synchronized Map<String, BigDecimal> getBalances(String accountId) {
        String prefix = accountId + "|";
        return balances.entrySet().stream()
                .filter(e -> e.getKey().startsWith(prefix))
                .collect(Collectors.toMap(e -> e.getKey().substring(prefix.length()),
                        Map.Entry::getValue, (a, b) -> a, TreeMap::new));
    }

    public synchronized ProcessingReport getReport() {
        Map<String, Long> counts = new TreeMap<>();
        for (Job job : bySequence.values()) {
            counts.merge(job.status.name(), 1L, Long::sum);
        }
        List<Long> waiting = bySequence.tailMap(nextSequence, true).values().stream()
                .filter(job -> job.status == TransactionStatus.PENDING_ORDER || job.status == TransactionStatus.RECEIVED)
                .map(job -> job.request.sequenceNumber()).toList();
        Long nextAvailable = bySequence.ceilingKey(nextSequence);
        Long missing = nextAvailable != null && nextAvailable > nextSequence ? nextSequence : null;
        Map<String, String> balancesAsStrings = new TreeMap<>();
        balances.forEach((k, v) -> balancesAsStrings.put(k, v.toPlainString()));
        return new ProcessingReport(submissions, duplicates, conflicts, invalids, retries,
                ledger.size(), nextSequence, missing, waiting, Map.copyOf(counts),
                Map.copyOf(balancesAsStrings));
    }

    private String validationProblem(TransactionRequest req) {
        if (!safeIdentifier(req.accountId())) return "accountId must be 1-80 letters/digits/._:-";
        if (blank(req.transactionType()) ||
                !(req.transactionType().equalsIgnoreCase("CREDIT") ||
                  req.transactionType().equalsIgnoreCase("DEBIT"))) {
            return "transactionType must be CREDIT or DEBIT";
        }
        if (req.amount() == null || req.amount().signum() <= 0) return "amount must be positive";
        if (req.amount().scale() > 2) return "amount supports at most two decimal places";
        if (blank(req.currency())) return "currency is required";
        try {
            Currency.getInstance(req.currency().trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException ex) {
            return "invalid ISO-4217 currency";
        }
        if (req.simulateTransientFailures() != null &&
                (req.simulateTransientFailures() < 0 || req.simulateTransientFailures() > 10)) {
            return "simulateTransientFailures must be 0..10";
        }
        return null;
    }

    private static boolean sameBusinessTransaction(TransactionRequest a, TransactionRequest b) {
        return Objects.equals(a.sequenceNumber(), b.sequenceNumber())
                && norm(a.accountId()).equals(norm(b.accountId()))
                && normUpper(a.transactionType()).equals(normUpper(b.transactionType()))
                && amountsEqual(a.amount(), b.amount())
                && normUpper(a.currency()).equals(normUpper(b.currency()));
    }

    private static boolean amountsEqual(BigDecimal a, BigDecimal b) {
        return a == null ? b == null : b != null && a.compareTo(b) == 0;
    }
    private static String norm(String s) { return s == null ? "" : s.trim(); }
    private static String normUpper(String s) { return norm(s).toUpperCase(Locale.ROOT); }
    private static boolean blank(String s) { return s == null || s.isBlank(); }
    private static boolean safeIdentifier(String s) {
        return s != null && s.matches("[A-Za-z0-9._:-]{1,80}");
    }
    private static String accountCurrencyKey(String account, String currency) {
        return account.trim() + "|" + currency.trim().toUpperCase(Locale.ROOT);
    }
    private static SubmissionResponse result(TransactionRequest req, TransactionStatus status,
                                             String message, int httpCode) {
        return new SubmissionResponse(req == null ? null : req.transactionId(),
                req == null ? null : req.requestId(), req == null ? null : req.sequenceNumber(),
                status, message, httpCode);
    }

    private static final class Job {
        final TransactionRequest request;
        TransactionStatus status;
        int attempts;
        String lastError;
        Instant nextAttempt = Instant.EPOCH;
        Job(TransactionRequest request, TransactionStatus status) {
            this.request = request;
            this.status = status;
        }
    }
}
