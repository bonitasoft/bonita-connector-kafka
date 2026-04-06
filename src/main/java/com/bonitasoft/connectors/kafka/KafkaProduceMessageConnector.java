package com.bonitasoft.connectors.kafka;

import com.bonitasoft.connectors.kafka.model.ProduceMessageResult;
import lombok.extern.slf4j.Slf4j;

/**
 * Kafka producer connector -- sends a message to a Kafka topic using KafkaProducer.send().
 */
@Slf4j
public class KafkaProduceMessageConnector extends AbstractKafkaConnector {

    static final String INPUT_BOOTSTRAP_SERVERS = "bootstrapServers";
    static final String INPUT_SECURITY_PROTOCOL = "securityProtocol";
    static final String INPUT_SASL_MECHANISM = "saslMechanism";
    static final String INPUT_SASL_USERNAME = "saslUsername";
    static final String INPUT_SASL_PASSWORD = "saslPassword";
    static final String INPUT_TOPIC = "topic";
    static final String INPUT_MESSAGE_KEY = "messageKey";
    static final String INPUT_MESSAGE_VALUE = "messageValue";
    static final String INPUT_TIMEOUT_SECONDS = "timeoutSeconds";
    static final String INPUT_MAX_RETRIES = "maxRetries";

    static final String OUTPUT_DESTINATION = "destination";
    static final String OUTPUT_PARTITION = "partition";
    static final String OUTPUT_OFFSET = "offset";
    static final String OUTPUT_TIMESTAMP = "timestamp";

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
                .messageKey(readStringInput(INPUT_MESSAGE_KEY))
                .messageValue(readStringInput(INPUT_MESSAGE_VALUE))
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
        if (config.getMessageValue() == null || config.getMessageValue().isBlank()) {
            throw new IllegalArgumentException("messageValue is mandatory");
        }
    }

    @Override
    protected void doExecute() throws KafkaException {
        log.info("Executing KafkaProduceMessage connector");

        ProduceMessageResult result = client.kafkaProduceMessage(
                configuration.getTopic(),
                configuration.getMessageKey(),
                configuration.getMessageValue()
        );

        setOutputParameter(OUTPUT_DESTINATION, result.destination());
        setOutputParameter(OUTPUT_PARTITION, result.partition());
        setOutputParameter(OUTPUT_OFFSET, result.offset());
        setOutputParameter(OUTPUT_TIMESTAMP, result.timestamp());

        log.info("KafkaProduceMessage connector executed successfully");
    }
}
