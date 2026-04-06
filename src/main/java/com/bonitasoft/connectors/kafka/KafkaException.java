package com.bonitasoft.connectors.kafka;

/**
 * Typed exception for Kafka/RabbitMQ connector operations.
 */
public class KafkaException extends Exception {

    private final int statusCode;
    private final boolean retryable;

    public KafkaException(String message) {
        super(message);
        this.statusCode = -1;
        this.retryable = false;
    }

    public KafkaException(String message, Throwable cause) {
        super(message, cause);
        this.statusCode = -1;
        this.retryable = false;
    }

    public KafkaException(String message, int statusCode, boolean retryable) {
        super(message);
        this.statusCode = statusCode;
        this.retryable = retryable;
    }

    public KafkaException(String message, int statusCode, boolean retryable, Throwable cause) {
        super(message, cause);
        this.statusCode = statusCode;
        this.retryable = retryable;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public boolean isRetryable() {
        return retryable;
    }
}
