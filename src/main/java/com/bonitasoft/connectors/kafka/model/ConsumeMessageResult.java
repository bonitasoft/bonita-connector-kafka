package com.bonitasoft.connectors.kafka.model;

/**
 * Immutable result record for consume operations.
 */
public record ConsumeMessageResult(
    int messageCount,
    String messagesJson,
    String source,
    String consumerGroup
) {}
