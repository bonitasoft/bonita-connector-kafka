package com.bonitasoft.connectors.kafka;

import lombok.Builder;
import lombok.Data;

/**
 * Configuration for Kafka/RabbitMQ connector.
 * Holds all connection and operation parameters for both brokers.
 */
@Data
@Builder
public class KafkaConfiguration {

    // === Broker selection ===
    private String brokerType; // "kafka" or "rabbitmq"

    // === Common connection ===
    private String bootstrapServers; // Kafka: host:port, RabbitMQ: amqp(s)://host:port

    // === Kafka SASL_SSL auth ===
    private String saslMechanism; // PLAIN, SCRAM-SHA-256, SCRAM-SHA-512
    private String saslUsername;
    private String saslPassword;
    private String securityProtocol; // SASL_SSL, SASL_PLAINTEXT, PLAINTEXT, SSL

    // === RabbitMQ auth ===
    private String rabbitmqUsername;
    private String rabbitmqPassword;
    private String rabbitmqVirtualHost;
    @Builder.Default
    private boolean rabbitmqSsl = false;

    // === Kafka-specific ===
    private String topic;
    private String consumerGroup;
    private String messageKey;
    private String messageValue;
    @Builder.Default
    private String keySerializer = "org.apache.kafka.common.serialization.StringSerializer";
    @Builder.Default
    private String valueSerializer = "org.apache.kafka.common.serialization.StringSerializer";
    @Builder.Default
    private String keyDeserializer = "org.apache.kafka.common.serialization.StringDeserializer";
    @Builder.Default
    private String valueDeserializer = "org.apache.kafka.common.serialization.StringDeserializer";
    @Builder.Default
    private String autoOffsetReset = "earliest";

    // === RabbitMQ-specific ===
    private String exchange;
    private String routingKey;
    private String queue;
    private String messageBody;
    @Builder.Default
    private String contentType = "application/json";
    @Builder.Default
    private boolean autoAck = true;

    // === Event (broker-agnostic) ===
    private String destination; // topic or exchange depending on broker
    private String jsonPayload;

    // === Advanced ===
    @Builder.Default
    private int timeoutSeconds = 30;

    @Builder.Default
    private int maxRetries = 3;

    @Builder.Default
    private long pollDurationMs = 5000L;

    @Builder.Default
    private int maxRecords = 10;
}
