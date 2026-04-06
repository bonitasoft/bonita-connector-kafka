package com.bonitasoft.connectors.kafka;

import com.bonitasoft.connectors.kafka.model.ConsumeMessageResult;
import lombok.extern.slf4j.Slf4j;

/**
 * Kafka consumer connector -- polls messages from a Kafka topic with consumer group.
 */
@Slf4j
public class KafkaConsumeMessageConnector extends AbstractKafkaConnector {

    static final String INPUT_BOOTSTRAP_SERVERS = "bootstrapServers";
    static final String INPUT_SECURITY_PROTOCOL = "securityProtocol";
    static final String INPUT_SASL_MECHANISM = "saslMechanism";
    static final String INPUT_SASL_USERNAME = "saslUsername";
    static final String INPUT_SASL_PASSWORD = "saslPassword";
    static final String INPUT_TOPIC = "topic";
    static final String INPUT_CONSUMER_GROUP = "consumerGroup";
    static final String INPUT_POLL_DURATION_MS = "pollDurationMs";
    static final String INPUT_MAX_RECORDS = "maxRecords";
    static final String INPUT_AUTO_OFFSET_RESET = "autoOffsetReset";
    static final String INPUT_TIMEOUT_SECONDS = "timeoutSeconds";
    static final String INPUT_MAX_RETRIES = "maxRetries";

    static final String OUTPUT_MESSAGE_COUNT = "messageCount";
    static final String OUTPUT_MESSAGES_JSON = "messagesJson";
    static final String OUTPUT_SOURCE = "source";
    static final String OUTPUT_CONSUMER_GROUP = "consumerGroupOut";

    @Override
    protected KafkaConfiguration buildConfiguration() {
        return KafkaConfiguration.builder()
                .brokerType("kafka")
                .bootstrapServers(readStringInput(INPUT_BOOTSTRAP_SERVERS))
                .securityProtocol(readStringInput(INPUT_SECURITY_PROTOCOL, "PLAINTEXT"))
                .saslMechanism(readStringInput(INPUT_SASL_MECHANISM))
                .saslUsername(readStringInput(INPUT_SASL_USERNAME))
                .saslPassword(readStringInput(INPUT_SASL_PASSWORD))
                .topic(readStringInput(INPUT_TOPIC))
                .consumerGroup(readStringInput(INPUT_CONSUMER_GROUP))
                .pollDurationMs(readLongInput(INPUT_POLL_DURATION_MS, 5000L))
                .maxRecords(readIntegerInput(INPUT_MAX_RECORDS, 10))
                .autoOffsetReset(readStringInput(INPUT_AUTO_OFFSET_RESET, "earliest"))
                .timeoutSeconds(readIntegerInput(INPUT_TIMEOUT_SECONDS, 30))
                .maxRetries(readIntegerInput(INPUT_MAX_RETRIES, 3))
                .build();
    }

    @Override
    protected void validateConfiguration(KafkaConfiguration config) {
        super.validateConfiguration(config);
        if (config.getTopic() == null || config.getTopic().isBlank()) {
            throw new IllegalArgumentException("topic is mandatory");
        }
        if (config.getConsumerGroup() == null || config.getConsumerGroup().isBlank()) {
            throw new IllegalArgumentException("consumerGroup is mandatory");
        }
    }

    @Override
    protected void doExecute() throws KafkaException {
        log.info("Executing KafkaConsumeMessage connector");

        ConsumeMessageResult result = client.kafkaConsumeMessage(
                configuration.getTopic(),
                configuration.getConsumerGroup(),
                configuration.getPollDurationMs(),
                configuration.getMaxRecords()
        );

        setOutputParameter(OUTPUT_MESSAGE_COUNT, result.messageCount());
        setOutputParameter(OUTPUT_MESSAGES_JSON, result.messagesJson());
        setOutputParameter(OUTPUT_SOURCE, result.source());
        setOutputParameter(OUTPUT_CONSUMER_GROUP, result.consumerGroup());

        log.info("KafkaConsumeMessage connector executed successfully");
    }
}
