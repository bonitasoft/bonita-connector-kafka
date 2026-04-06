package com.bonitasoft.connectors.kafka;

import com.bonitasoft.connectors.kafka.model.ConsumerStatusResult;
import lombok.extern.slf4j.Slf4j;

/**
 * Monitor consumer lag (Kafka) or queue depth (RabbitMQ).
 */
@Slf4j
public class EventGetConsumerStatusConnector extends AbstractKafkaConnector {

    static final String INPUT_BROKER_TYPE = "brokerType";
    static final String INPUT_BOOTSTRAP_SERVERS = "bootstrapServers";
    static final String INPUT_SECURITY_PROTOCOL = "securityProtocol";
    static final String INPUT_SASL_MECHANISM = "saslMechanism";
    static final String INPUT_SASL_USERNAME = "saslUsername";
    static final String INPUT_SASL_PASSWORD = "saslPassword";
    static final String INPUT_RABBITMQ_USERNAME = "rabbitmqUsername";
    static final String INPUT_RABBITMQ_PASSWORD = "rabbitmqPassword";
    static final String INPUT_RABBITMQ_VIRTUAL_HOST = "rabbitmqVirtualHost";
    static final String INPUT_RABBITMQ_SSL = "rabbitmqSsl";
    static final String INPUT_TOPIC_OR_QUEUE = "topicOrQueue";
    static final String INPUT_CONSUMER_GROUP = "consumerGroup";
    static final String INPUT_TIMEOUT_SECONDS = "timeoutSeconds";
    static final String INPUT_MAX_RETRIES = "maxRetries";

    static final String OUTPUT_BROKER_TYPE = "brokerTypeOut";
    static final String OUTPUT_SOURCE = "source";
    static final String OUTPUT_CONSUMER_GROUP = "consumerGroupOut";
    static final String OUTPUT_LAG_OR_DEPTH = "lagOrDepth";
    static final String OUTPUT_PARTITIONS_OR_CONSUMERS = "partitionsOrConsumers";
    static final String OUTPUT_CONNECTED = "connected";

    @Override
    protected KafkaConfiguration buildConfiguration() {
        return KafkaConfiguration.builder()
                .brokerType(readStringInput(INPUT_BROKER_TYPE))
                .bootstrapServers(readStringInput(INPUT_BOOTSTRAP_SERVERS))
                .securityProtocol(readStringInput(INPUT_SECURITY_PROTOCOL, "PLAINTEXT"))
                .saslMechanism(readStringInput(INPUT_SASL_MECHANISM))
                .saslUsername(readStringInput(INPUT_SASL_USERNAME))
                .saslPassword(readStringInput(INPUT_SASL_PASSWORD))
                .rabbitmqUsername(readStringInput(INPUT_RABBITMQ_USERNAME))
                .rabbitmqPassword(readStringInput(INPUT_RABBITMQ_PASSWORD))
                .rabbitmqVirtualHost(readStringInput(INPUT_RABBITMQ_VIRTUAL_HOST, "/"))
                .rabbitmqSsl(readBooleanInput(INPUT_RABBITMQ_SSL, false))
                .topic(readStringInput(INPUT_TOPIC_OR_QUEUE))
                .consumerGroup(readStringInput(INPUT_CONSUMER_GROUP))
                .timeoutSeconds(readIntegerInput(INPUT_TIMEOUT_SECONDS, 30))
                .maxRetries(readIntegerInput(INPUT_MAX_RETRIES, 3))
                .build();
    }

    @Override
    protected void validateConfiguration(KafkaConfiguration config) {
        super.validateConfiguration(config);
        if (config.getTopic() == null || config.getTopic().isBlank()) {
            throw new IllegalArgumentException("topicOrQueue is mandatory");
        }
    }

    @Override
    protected void doExecute() throws KafkaException {
        log.info("Executing EventGetConsumerStatus connector for broker: {}", configuration.getBrokerType());

        ConsumerStatusResult result = client.getConsumerStatus(
                configuration.getBrokerType(),
                configuration.getTopic(),
                configuration.getConsumerGroup()
        );

        setOutputParameter(OUTPUT_BROKER_TYPE, result.brokerType());
        setOutputParameter(OUTPUT_SOURCE, result.source());
        setOutputParameter(OUTPUT_CONSUMER_GROUP, result.consumerGroup());
        setOutputParameter(OUTPUT_LAG_OR_DEPTH, result.lagOrDepth());
        setOutputParameter(OUTPUT_PARTITIONS_OR_CONSUMERS, result.partitionsOrConsumers());
        setOutputParameter(OUTPUT_CONNECTED, result.connected());

        log.info("EventGetConsumerStatus connector executed successfully");
    }
}
