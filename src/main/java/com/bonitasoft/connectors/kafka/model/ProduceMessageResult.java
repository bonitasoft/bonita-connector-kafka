package com.bonitasoft.connectors.kafka.model;

/**
 * Immutable result record for produce/publish operations.
 */
public record ProduceMessageResult(
    String destination,
    int partition,
    long offset,
    long timestamp,
    boolean sent
) {}
