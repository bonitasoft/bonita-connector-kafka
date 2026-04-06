package com.bonitasoft.connectors.kafka;

import com.bonitasoft.connectors.kafka.model.EventPublishResult;
import lombok.extern.slf4j.Slf4j;

/**
 * Broker-agnostic JSON event publish connector.
 * Routes to Kafka or RabbitMQ based on brokerType input.
 */
@Slf4j
public class EventPublishJsonConnector extends AbstractKafkaConnector {

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
    static final String INPUT_DESTINATION = "destination";
    static final String INPUT_JSON_PAYLOAD = "jsonPayload";
    static final String INPUT_TIMEOUT_SECONDS = "timeoutSeconds";
    static final String INPUT_MAX_RETRIES = "maxRetries";

    static final String OUTPUT_BROKER_TYPE = "brokerTypeOut";
    static final String OUTPUT_DESTINATION = "destinationOut";
    static final String OUTPUT_SENT = "sent";
    static final String OUTPUT_TIMESTAMP = "timestamp";

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
                .destination(readStringInput(INPUT_DESTINATION))
                .jsonPayload(readStringInput(INPUT_JSON_PAYLOAD))
                .timeoutSeconds(readIntegerInput(INPUT_TIMEOUT_SECONDS, 30))
                .maxRetries(readIntegerInput(INPUT_MAX_RETRIES, 3))
                .build();
    }

    @Override
    protected void validateConfiguration(KafkaConfiguration config) {
        super.validateConfiguration(config);
        if (config.getDestination() == null || config.getDestination().isBlank()) {
            throw new IllegalArgumentException("destination is mandatory");
        }
        if (config.getJsonPayload() == null || config.getJsonPayload().isBlank()) {
            throw new IllegalArgumentException("jsonPayload is mandatory");
        }
    }

    @Override
    protected void doExecute() throws KafkaException {
        log.info("Executing EventPublishJson connector for broker: {}", configuration.getBrokerType());

        EventPublishResult result = client.eventPublishJson(
                configuration.getBrokerType(),
                configuration.getDestination(),
                configuration.getJsonPayload()
        );

        setOutputParameter(OUTPUT_BROKER_TYPE, result.brokerType());
        setOutputParameter(OUTPUT_DESTINATION, result.destination());
        setOutputParameter(OUTPUT_SENT, result.sent());
        setOutputParameter(OUTPUT_TIMESTAMP, result.timestamp());

        log.info("EventPublishJson connector executed successfully");
    }
}
