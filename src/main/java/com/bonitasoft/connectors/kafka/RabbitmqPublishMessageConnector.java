package com.bonitasoft.connectors.kafka;

import com.bonitasoft.connectors.kafka.model.ProduceMessageResult;
import lombok.extern.slf4j.Slf4j;

/**
 * RabbitMQ publish connector -- sends a message to an exchange via Channel.basicPublish().
 */
@Slf4j
public class RabbitmqPublishMessageConnector extends AbstractKafkaConnector {

    static final String INPUT_BOOTSTRAP_SERVERS = "bootstrapServers";
    static final String INPUT_RABBITMQ_USERNAME = "rabbitmqUsername";
    static final String INPUT_RABBITMQ_PASSWORD = "rabbitmqPassword";
    static final String INPUT_RABBITMQ_VIRTUAL_HOST = "rabbitmqVirtualHost";
    static final String INPUT_RABBITMQ_SSL = "rabbitmqSsl";
    static final String INPUT_EXCHANGE = "exchange";
    static final String INPUT_ROUTING_KEY = "routingKey";
    static final String INPUT_MESSAGE_BODY = "messageBody";
    static final String INPUT_CONTENT_TYPE = "contentType";
    static final String INPUT_TIMEOUT_SECONDS = "timeoutSeconds";
    static final String INPUT_MAX_RETRIES = "maxRetries";

    static final String OUTPUT_DESTINATION = "destination";
    static final String OUTPUT_PARTITION = "partition";
    static final String OUTPUT_OFFSET = "offset";
    static final String OUTPUT_TIMESTAMP = "timestamp";

    @Override
    protected KafkaConfiguration buildConfiguration() {
        return KafkaConfiguration.builder()
                .brokerType("rabbitmq")
                .bootstrapServers(readStringInput(INPUT_BOOTSTRAP_SERVERS))
                .rabbitmqUsername(readStringInput(INPUT_RABBITMQ_USERNAME))
                .rabbitmqPassword(readStringInput(INPUT_RABBITMQ_PASSWORD))
                .rabbitmqVirtualHost(readStringInput(INPUT_RABBITMQ_VIRTUAL_HOST, "/"))
                .rabbitmqSsl(readBooleanInput(INPUT_RABBITMQ_SSL, false))
                .exchange(readStringInput(INPUT_EXCHANGE, ""))
                .routingKey(readStringInput(INPUT_ROUTING_KEY))
                .messageBody(readStringInput(INPUT_MESSAGE_BODY))
                .contentType(readStringInput(INPUT_CONTENT_TYPE, "application/json"))
                .timeoutSeconds(readIntegerInput(INPUT_TIMEOUT_SECONDS, 30))
                .maxRetries(readIntegerInput(INPUT_MAX_RETRIES, 3))
                .build();
    }

    @Override
    protected void validateConfiguration(KafkaConfiguration config) {
        super.validateConfiguration(config);
        if (config.getRoutingKey() == null || config.getRoutingKey().isBlank()) {
            throw new IllegalArgumentException("routingKey is mandatory");
        }
        if (config.getMessageBody() == null || config.getMessageBody().isBlank()) {
            throw new IllegalArgumentException("messageBody is mandatory");
        }
    }

    @Override
    protected void doExecute() throws KafkaException {
        log.info("Executing RabbitmqPublishMessage connector");

        ProduceMessageResult result = client.rabbitmqPublishMessage(
                configuration.getExchange(),
                configuration.getRoutingKey(),
                configuration.getMessageBody(),
                configuration.getContentType()
        );

        setOutputParameter(OUTPUT_DESTINATION, result.destination());
        setOutputParameter(OUTPUT_PARTITION, result.partition());
        setOutputParameter(OUTPUT_OFFSET, result.offset());
        setOutputParameter(OUTPUT_TIMESTAMP, result.timestamp());

        log.info("RabbitmqPublishMessage connector executed successfully");
    }
}
