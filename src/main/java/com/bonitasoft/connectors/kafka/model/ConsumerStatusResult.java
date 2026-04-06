package com.bonitasoft.connectors.kafka.model;

/**
 * Immutable result record for consumer status / monitoring.
 */
public record ConsumerStatusResult(
    String brokerType,
    String source,
    String consumerGroup,
    long lagOrDepth,
    int partitionsOrConsumers,
    boolean connected
) {}
