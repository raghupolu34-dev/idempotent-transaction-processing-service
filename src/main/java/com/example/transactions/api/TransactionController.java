package com.example.transactions.api;

import com.example.transactions.core.TransactionEngine;
import com.example.transactions.domain.*;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api")
public class TransactionController {
    private final TransactionEngine engine;
    public TransactionController(TransactionEngine engine) { this.engine = engine; }

    @PostMapping("/transactions")
    public ResponseEntity<SubmissionResponse> submit(@RequestBody TransactionRequest request) {
        var reply = engine.submit(request);
        return ResponseEntity.status(reply.httpStatus()).body(reply);
    }

    // All records are handled independently; each response contains its own HTTP-equivalent status.
    @PostMapping("/transactions/batch")
    public List<SubmissionResponse> submitBatch(@RequestBody List<TransactionRequest> requests) {
        return requests.stream().map(engine::submit).toList();
    }

    @GetMapping("/transactions/{transactionId}")
    public ResponseEntity<TransactionView> transaction(@PathVariable String transactionId) {
        var view = engine.getTransaction(transactionId);
        return view == null ? ResponseEntity.notFound().build() : ResponseEntity.ok(view);
    }

    @GetMapping("/accounts/{accountId}/balances")
    public Map<String, BigDecimal> balances(@PathVariable String accountId) {
        return engine.getBalances(accountId);
    }

    @GetMapping("/ledger")
    public List<LedgerEntry> ledger() { return engine.getLedger(); }

    @GetMapping("/summary")
    public ProcessingReport summary() { return engine.getReport(); }

    // Deliberately development-only demo control; protect/remove in real deployments.
    @PostMapping("/admin/process")
    public Map<String, Object> drain(@RequestParam(defaultValue = "100") int limit) {
        int attempts = engine.processReady(limit);
        return Map.of("workAttempts", attempts, "report", engine.getReport());
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, String>> badParam(IllegalArgumentException ex) {
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of("error", ex.getMessage()));
    }
}
