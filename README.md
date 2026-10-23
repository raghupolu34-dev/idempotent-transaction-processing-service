# Idempotent Background Transaction Processing Service

**FDE L2 practical assessment | Java 17 | Spring Boot 3.5.7 | Maven | JUnit 5**

A lightweight, production-aware **background transaction processor** that safely ingests unordered and repeated financial events, validates them, applies each business transaction **at most once within one running JVM**, and exposes a processing report. The transaction engine has **no framework dependencies** and can be tested with the JDK alone. The Spring Boot adapter supplies REST ingestion and a scheduled worker.

> **Important scope limitation:** This is an **in-memory assessment implementation**, not a production payment system. Data and deduplication history disappear on restart. For durable, multi-instance exactly-once effects, see [Production hardening](#production-hardening).

## 1. Requirements implemented

| Assessment requirement | Solution |
| --- | --- |
| Replaceable input source | JSON REST endpoints + pure Java `TransactionEngine.submit()` |
| Validate mandatory fields | ID, request ID, sequence, account, type, amount, currency, retry simulation |
| Transaction lifecycle | `RECEIVED → PROCESSING → PROCESSED`; `PENDING_ORDER`, `RETRY_PENDING`, `FAILED` |
| Business idempotency | `transactionId` indexes the original transaction and its normalized business payload |
| Retry/request deduplication | New request IDs for the same transaction return `DUPLICATE` with no effects |
| Duplicate request ID protection | Same `requestId` cannot be used for another transaction |
| Out-of-order handling | **Option A:** globally sequenced input feed; future events wait for missing sequences |
| Simulated transient failure | `simulateTransientFailures` (0–10) on an event |
| Bounded retry | 3 max attempts by default, exponential backoff, terminal `FAILED` state |
| Consistency | Single synchronized critical section covers state, balance, and ledger update |
| Operational visibility | Summary metrics, attempt counts, error reasons, ledger, balances, Actuator health |
| Tests | JUnit 5 test suite + no-dependency Java smoke tests |
| Malformed record isolation | Invalid *sequenced* event receives 422 and is recorded as terminal failed; later seqs continue |

## 2. Architecture

```text
     HTTP client / source adapter
                |
         POST /api/transactions
         POST /api/transactions/batch
                |
                v
       TransactionController
                |
                v
       TransactionEngine (Java 17)
          |             |
     Validate +      Idempotency lookup
    reserve seq        transactionId / requestId
          \             /
           v           v
        Sorted pending jobs (TreeMap<sequence, Job>)
                |
         BackgroundTransactionWorker
         (@Scheduled; configurable interval)
                |
      Ordered state machine & bounded retry
                |
       Atomically apply balance delta
                  + append ledger entry
                |
     GET /api/summary, /api/ledger, /api/accounts/{id}/balances
```

**Separation of concerns:** The engine implements domain rules. The controller provides a replaceable transport. The scheduled worker is a replaceable execution adapter, so a queue consumer or database poller can be integrated later without rewriting the core business rules.

## 3. Run locally

### Prerequisites

- JDK **17+** (`java -version`)
- Apache Maven **3.9+** for Spring Boot (`mvn -version`)
- No external database, Kafka, Docker, or paid services required.

### Spring Boot service

```bash
mvn clean test
mvn spring-boot:run
```

App: **http://localhost:8080**. Health endpoint: `http://localhost:8080/actuator/health`.

Or build/run the jar:

```bash
mvn clean package
java -jar target/idempotent-transaction-service-1.0.0.jar
```

### No Maven available? Try the core engine standalone

Linux/macOS/Git Bash:

```bash
bash scripts/run-core-demo.sh
```

This command compiles the domain and core using `javac --release 17`, runs **five assertion-based smoke scenarios**, and executes the original six-row assessment example. It does **not** start the REST server.

### Docker (optional)

```bash
docker build -t idempotent-transactions .
docker run -p 8080:8080 idempotent-transactions
```

## 4. Quick API walkthrough

**Start Spring Boot**, then submit the supplied example batch.

Linux/macOS/Git Bash:

```bash
curl -i -X POST http://localhost:8080/api/transactions/batch \
  -H 'Content-Type: application/json' \
  --data-binary @samples/transactions.json
```

Windows PowerShell:

```powershell
Invoke-RestMethod -Uri 'http://localhost:8080/api/transactions/batch' `
  -Method Post -ContentType 'application/json' `
  -Body (Get-Content -Raw '.\samples\transactions.json')
```

The background worker runs every **1 second** by default and retries a transient failure after **1 second**. Wait for it to finish and query the summary, or advance eligible work manually using the demo-only endpoint:

```bash
curl -X POST 'http://localhost:8080/api/admin/process?limit=100'
curl http://localhost:8080/api/summary
curl http://localhost:8080/api/ledger
curl http://localhost:8080/api/accounts/ACC-201/balances
curl http://localhost:8080/api/transactions/TXN-2005
```

For an individual request:

```bash
curl -i -X POST http://localhost:8080/api/transactions \
  -H 'Content-Type: application/json' \
  -d '{"transactionId":"MY-TXN-1","requestId":"MY-REQ-1","sequenceNumber":6,"accountId":"ACC-201","transactionType":"CREDIT","amount":50,"currency":"INR"}'
```

**Important:** The above new example uses **sequence 6** because the six-row sample reserves sequence slots **1–5**. On a fresh server with empty state, start at **sequence 1**. Re-sending the six-row batch on the same running server counts more duplicates but **does not** create additional ledger entries.

### Status and response rules

| Input | HTTP/record status | Business effect |
| --- | --- | --- |
| New valid transaction | `202 RECEIVED` or `PENDING_ORDER` | Queued, no effect until worker executes |
| Duplicate transaction, identical business payload | `200 DUPLICATE` | Never applied again |
| Same `transactionId`, different business payload | `409 CONFLICT` | Rejected, original unchanged |
| Reused `requestId` on different business transaction | `409 CONFLICT` | Rejected |
| No usable transaction ID, request ID or positive sequence | `400 REJECTED` | Rejected; cannot reserve an ordered slot |
| Invalid *otherwise identifiable* event (e.g. negative amount) | `422 FAILED` | Terminal/audited slot, no ledger effect |
| Simulated transient error | `RETRY_PENDING` then `PROCESSED` or `FAILED` | No effect during failed attempts |

For **batch requests**, the API responds `200` with a JSON array; each element contains `httpStatus` and `status`, so one invalid record does not fail the full batch.

## 5. Demonstration with assignment sample

Six input requests intentionally arrive in this order:

1. `TXN-2002`, sequence 2, debit 40 — initially held because 1 is missing.
2. `TXN-2001`, sequence 1, credit 100 — enables sequence 2.
3. `TXN-2002`, sequence 2, debit 40, different request ID — **duplicate**.
4. `TXN-2003`, sequence 3, credit 25 — succeeds.
5. `TXN-2004`, sequence 4, debit **-10** — **fails validation**, audited and skipped.
6. `TXN-2005`, sequence 5, credit 500 — fails once on purpose, then **succeeds on attempt 2**.

**Expected final result once retries finish:**

```text
Processed unique transactions: 4
Duplicate submissions:         1
Invalid transactions:          1
Retry attempts scheduled:      1
Ledger entries:                4
Next expected global sequence: 6
ACC-201 INR balance:           85  (100 - 40 + 25)
ACC-203 INR balance:           500
```

See `samples/transactions.json` and `docs/sample-output.txt` for reproducible sample data and observed standalone output.

## 6. Design decisions and trade-offs

### Idempotency and duplicate conflicts

`transactionId` is the **unique business identity**. A normalized signature (sequence, account, type, numeric amount, currency) identifies the same business transaction even when the request ID changes or `100` becomes `100.00`. `requestId` provides additional message-level deduplication. Duplicate acknowledgements **do not overwrite** the original transaction's state, so an already processed record remains `PROCESSED`.

A conflicting payload using an already-seen ID is **409** rather than silently accepting different money movement. The accounting effect is committed once by the synchronized engine. The engine does not call external downstream services.

### Out-of-order processing (assessment Option A)

The sample's sequence values are **globally monotonic across accounts**. The engine therefore uses one global `nextExpectedSequence`, initially 1. A later sequence is buffered until all previous numbered input positions have become terminal. This makes ordering straightforward and testable; **the cost is global head-of-line blocking** when one sequence is missing or retrying. `/api/summary` displays the first gap and waiting sequences. No timeout auto-advances a *missing* sequence; an operator must reconcile it.

A **present but malformed** event with enough trustworthy identity and sequence is stored as terminal `FAILED` and advances its input slot, enabling later transactions. For a real feed, consuming malformed sequence positions requires trusted-producer contracts, audit trails, and operator reconciliation; otherwise attackers could force an incorrect skip. Alternative production designs would use partition/account-level sequences, causal ordering, or explicit gap management rather than one global feed.

### Retry model and safe failure handling

`simulateTransientFailures=N` makes the first N processing attempts fail **before any balance or ledger side effect**. The worker uses max **3** attempts by default and exponential retry delays (**1 s, 2 s**). On success it adds one ledger entry. After the retry limit the transaction becomes terminal `FAILED` and the ordered feed progresses. Individual attempts and failure reasons are visible from `GET /api/transactions/{id}`.

We intentionally avoid simulating a failure *after* an external payment system applied an effect; that would require external idempotency keys and durable reconciliation. Real retries are only safe when the downstream side effect is itself idempotent or coordinated through a transaction/outbox.

### Concurrency and atomicity

All operations on the engine are `synchronized`, so duplicate submissions and worker ticks cannot interleave a ledger update in **one JVM**. Account balances are tracked separately **per currency**, using `BigDecimal`. The assessment allows a negative balance for a debit (no overdraft checks); this is an explicit **domain assumption**, not a suggested policy for actual banking applications.

This local atomicity is **not** a distributed transaction and **not durable**. Multiple JVM replicas would each have independent state, and a process restart loses everything. The strongest guarantee here is **at-most-once effects while the single process stays alive**, not end-to-end exactly-once delivery.

### Validation and observability

- Required: nonblank transactionId, requestId, accountId, positive sequence, valid `CREDIT`/`DEBIT`, positive amount (maximum two decimal places), ISO-4217 currency.
- A request without a trustworthy positive sequence is rejected and cannot be used to complete a gap.
- A known transaction can be retrieved with its status, processing attempts, and safe error reason.
- The summary contains statuses, duplicate/conflict/invalid counters, retries, balances, and missing-sequence information.
- Application log lines report aggregate background work without printing customer personal information.
- Spring Boot Actuator exposes basic `/actuator/health`. Production should instrument richer metrics and trace IDs.

## 7. Automated test coverage

`mvn clean test` runs **11 JUnit 5 tests**:

1. Out-of-order sequence 2 waits for sequence 1.
2. Business duplicate with a new request ID causes no second effect.
3. Duplicate *while pending* is also detected.
4. Same transaction ID with different amount causes conflict.
5. Reused request ID across different transactions causes conflict.
6. Invalid negative amount does not stop later slots.
7. Transient failure is retried once without double-crediting.
8. Retry exhaustion results in `FAILED`; later slots continue.
9. Missing transaction sequence is rejected.
10. Balances are separated by currency.
11. Concurrent duplicate submissions stay single-effect within one JVM.

A separate JDK-only suite in `scripts/CoreSmokeTest.java` independently verifies the critical cases without downloading dependencies.

## 8. Production hardening

1. **Durability:** Replace the in-memory maps with PostgreSQL. Use `UNIQUE(transaction_id)`, `UNIQUE(request_id)` and `UNIQUE(feed_id, sequence_number)` constraints. Store current status, attempts, next retry time, and immutable ledger in persistent tables.
2. **Transactional effects:** Use `@Transactional` so inserting the ledger entry, updating the balance with appropriate locks/version checks, and marking the transaction `PROCESSED` succeed or roll back together. Enforce **unique ledger transaction ID** in the database.
3. **Horizontal scaling:** Move from JVM-wide locking to partitioned work by account or feed, database row locks (`SELECT ... FOR UPDATE SKIP LOCKED`), leases/fencing, or Kafka partitions with a documented ordering contract. Do not run this memory-only implementation in multiple replicas.
4. **Crash recovery:** Recover all `PROCESSING`/expired leased records on restart. Deduplication history must outlive queue redelivery/replay windows. Require backups and retention policies.
5. **Downstream side effects:** Adopt the transactional outbox pattern, idempotent consumer/inbox, and consumer-offset handling. If an external payment gateway is involved, pass an external idempotency key and reconcile unknown outcomes.
6. **Retry and poison messages:** Exponential backoff + jitter, retry classifications, dead-letter queue (DLQ), replay tools, and alerting after maximum retries.
7. **Ordering/gaps:** Alert on missing feed sequence; implement manual reconciliation/timeout escalation. Avoid unsafe automatic skips of unknown sequence numbers.
8. **Validation/security:** AuthN/AuthZ, TLS, request-size limits, rate limits, data classification, secret manager, audit log, and least-privilege roles. Protect/remove `/api/admin/process` from public access.
9. **Monitoring:** Micrometer counters, retry/queue lag histograms, structured correlation IDs, tracing, dashboards and SLO alerts.
10. **Domain correctness:** Introduce debit authorization/insufficient-funds policy, reversal references, FX rules, amount/currency precision, immutable audit records and integration tests on a real database.

## 9. AI-assisted development statement

**AI usage:** An AI coding assistant was used to accelerate the architecture outline, initial Java implementation, test cases, and documentation. **Validation performed in this environment:** compiled the pure Java engine with Java 21 targeting Java 17 and ran the five assertion-based smoke scenarios with a passing result. A developer should independently run `mvn clean verify` in their local environment and inspect/edit the code before submitting. **Avoid claiming unperformed testing or production guarantees.**

**Human review focus:** Check that idempotency is based on the **business transaction**, the original state is never overwritten by a duplicate receipt, amount comparison is numeric rather than string-based, the negative amount never reaches the ledger, and a transient failure happens before the atomic business write.

## 10. GitHub submission checklist

- [ ] Install Maven, run `mvn clean verify`, and confirm all JUnit tests pass.
- [ ] Run `mvn spring-boot:run` and test the `samples/transactions.json` API flow.
- [ ] Read the README, inspect the core service, and be prepared to explain trade-offs.
- [ ] Create a **public GitHub repository** and push these project files (not the ZIP alone).
- [ ] Copy the **public repository URL** and submit it to the recruiter within the email deadline.

Example push commands (after creating an empty repository on GitHub):

```bash
git init
git add .
git commit -m "Build idempotent background transaction processor"
git branch -M main
git remote add origin https://github.com/YOUR_USERNAME/idempotent-transaction-service.git
git push -u origin main
```

Replace `YOUR_USERNAME` with your GitHub username; this repository URL is only a **template**, not an existing link.
