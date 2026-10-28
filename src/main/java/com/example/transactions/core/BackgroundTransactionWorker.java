package com.example.transactions.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class BackgroundTransactionWorker {
    private static final Logger log = LoggerFactory.getLogger(BackgroundTransactionWorker.class);
    private final TransactionEngine engine;
    private final int batchSize;

    public BackgroundTransactionWorker(TransactionEngine engine,
            @Value("${app.processor.batch-size:100}") int batchSize) {
        this.engine = engine;
        this.batchSize = batchSize;
    }

    @Scheduled(fixedDelayString = "${app.processor.interval-ms:1000}")
    public void processDueTransactions() {
        int work = engine.processReady(batchSize);
        if (work > 0) {
            var report = engine.getReport();
            log.info("background_work={} processed={} retries={} invalid={} next_sequence={}",
                    work, report.processedLedgerEntries(), report.retryAttemptsScheduled(),
                    report.invalidRecords(), report.nextExpectedSequence());
        }
    }
}
