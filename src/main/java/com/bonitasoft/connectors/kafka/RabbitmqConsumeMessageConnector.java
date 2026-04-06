package com.bonitasoft.connectors.kafka;

import com.bonitasoft.connectors.kafka.model.ConsumeMessageResult;
import lombok.extern.slf4j.Slf4j;

/**
 * RabbitMQ consume connector -- retrieves a message from a queue via Channel.basicGet().
 */
@Slf4j
public class RabbitmqConsumeMessageConnector extends AbstractKafkaConnector {

    static final String INPUT_BOOTSTRAP_SERVERS = "bootstrapServers";
    static final String INPUT_RABBITMQ_USERNAME = "rabbitmqUsername";
    static final String INPUT_RABBITMQ_PASSWORD = "rabbitmqPassword";
    static final String INPUT_RABBITMQ_VIRTUAL_HOST = "rabbitmqVirtualHost";
    static final String INPUT_RABBITMQ_SSL = "rabbitmqSsl";
    static final String INPUT_QUEUE = "queue";
    static final String INPUT_AUTO_ACK = "autoAck";
    static final String INPUT_TIMEOUT_SECONDS = "timeoutSeconds";
    static final String INPUT_MAX_RETRIES = "maxRetries";

    static final String OUTPUT_MESSAGE_COUNT = "messageCount";
    static final String OUTPUT_MESSAGES_JSON = "messagesJson";
    static final String OUTPUT_SOURCE = "source";

    @Override
    protected KafkaConfiguration buildConfiguration() {
        return KafkaConfiguration.builder()
                .brokerType("rabbitmq")
                .bootstrapServers(readStringInput(INPUT_BOOTSTRAP_SERVERS))
                .rabbitmqUsername(readStringInput(INPUT_RABBITMQ_USERNAME))
                .rabbitmqPassword(readStringInput(INPUT_RABBITMQ_PASSWORD))
                .rabbitmqVirtualHost(readStringInput(INPUT_RABBITMQ_VIRTUAL_HOST, "/"))
                .rabbitmqSsl(readBooleanInput(INPUT_RABBITMQ_SSL, false))
                .queue(readStringInput(INPUT_QUEUE))
                .autoAck(readBooleanInput(INPUT_AUTO_ACK, true))
                .timeoutSeconds(readIntegerInput(INPUT_TIMEOUT_SECONDS, 30))
                .maxRetries(readIntegerInput(INPUT_MAX_RETRIES, 3))
                .build();
    }

    @Override
    protected void validateConfiguration(KafkaConfiguration config) {
        super.validateConfiguration(config);
        if (config.getQueue() == null || config.getQueue().isBlank()) {
            throw new IllegalArgumentException("queue is mandatory");
        }
    }

    @Override
    protected void doExecute() throws KafkaException {
        log.info("Executing RabbitmqConsumeMessage connector");

        ConsumeMessageResult result = client.rabbitmqConsumeMessage(
                configuration.getQueue(),
                configuration.isAutoAck()
        );

        setOutputParameter(OUTPUT_MESSAGE_COUNT, result.messageCount());
        setOutputParameter(OUTPUT_MESSAGES_JSON, result.messagesJson());
        setOutputParameter(OUTPUT_SOURCE, result.source());

        log.info("RabbitmqConsumeMessage connector executed successfully");
    }
}
