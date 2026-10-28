package com.example.transactions.config;

import com.example.transactions.core.TransactionEngine;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;
import java.time.Duration;

@Configuration
public class TransactionConfiguration {
    @Bean
    public TransactionEngine transactionEngine(
            @Value("${app.processor.max-attempts:3}") int maxAttempts,
            @Value("${app.processor.retry-delay-ms:1000}") long retryDelayMs) {
        return new TransactionEngine(Clock.systemUTC(), maxAttempts, Duration.ofMillis(retryDelayMs));
    }
}
