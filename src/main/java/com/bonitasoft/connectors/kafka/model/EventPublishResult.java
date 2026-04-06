package com.bonitasoft.connectors.kafka.model;

/**
 * Immutable result record for broker-agnostic event publish.
 */
public record EventPublishResult(
    String brokerType,
    String destination,
    boolean sent,
    long timestamp
) {}
