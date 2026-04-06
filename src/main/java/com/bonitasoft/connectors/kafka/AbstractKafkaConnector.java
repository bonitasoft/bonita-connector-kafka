package com.bonitasoft.connectors.kafka;

import java.util.Map;

import lombok.extern.slf4j.Slf4j;
import org.bonitasoft.engine.connector.AbstractConnector;
import org.bonitasoft.engine.connector.ConnectorException;
import org.bonitasoft.engine.connector.ConnectorValidationException;

/**
 * Abstract base connector for Kafka/RabbitMQ messaging.
 * Provides shared connection lifecycle management and error handling.
 */
@Slf4j
public abstract class AbstractKafkaConnector extends AbstractConnector {

    protected static final String OUTPUT_SUCCESS = "success";
    protected static final String OUTPUT_ERROR_MESSAGE = "errorMessage";

    protected KafkaConfiguration configuration;
    protected KafkaClient client;

    @Override
    public void validateInputParameters() throws ConnectorValidationException {
        try {
            this.configuration = buildConfiguration();
            validateConfiguration(this.configuration);
        } catch (IllegalArgumentException e) {
            throw new ConnectorValidationException(this, e.getMessage());
        }
    }

    @Override
    public void connect() throws ConnectorException {
        try {
            this.client = new KafkaClient(this.configuration);
            log.info("Kafka connector connected successfully");
        } catch (KafkaException e) {
            throw new ConnectorException("Failed to connect: " + e.getMessage(), e);
        }
    }

    @Override
    public void disconnect() throws ConnectorException {
        if (this.client != null) {
            this.client.close();
            this.client = null;
        }
    }

    @Override
    protected void executeBusinessLogic() throws ConnectorException {
        try {
            doExecute();
            setOutputParameter(OUTPUT_SUCCESS, true);
        } catch (KafkaException e) {
            log.error("Kafka connector execution failed: {}", e.getMessage(), e);
            setOutputParameter(OUTPUT_SUCCESS, false);
            setOutputParameter(OUTPUT_ERROR_MESSAGE, e.getMessage());
        } catch (Exception e) {
            log.error("Unexpected error in Kafka connector: {}", e.getMessage(), e);
            setOutputParameter(OUTPUT_SUCCESS, false);
            setOutputParameter(OUTPUT_ERROR_MESSAGE, "Unexpected error: " + e.getMessage());
        }
    }

    protected abstract void doExecute() throws KafkaException;

    protected abstract KafkaConfiguration buildConfiguration();

    protected void validateConfiguration(KafkaConfiguration config) {
        if (config.getBrokerType() == null || config.getBrokerType().isBlank()) {
            throw new IllegalArgumentException("brokerType is mandatory");
        }
        if (config.getBootstrapServers() == null || config.getBootstrapServers().isBlank()) {
            throw new IllegalArgumentException("bootstrapServers is mandatory");
        }
    }

    protected String readStringInput(String name) {
        Object value = getInputParameter(name);
        return value != null ? value.toString() : null;
    }

    protected String readStringInput(String name, String defaultValue) {
        String value = readStringInput(name);
        return (value != null && !value.isBlank()) ? value : defaultValue;
    }

    protected Boolean readBooleanInput(String name, boolean defaultValue) {
        Object value = getInputParameter(name);
        return value != null ? (Boolean) value : defaultValue;
    }

    protected Integer readIntegerInput(String name, int defaultValue) {
        Object value = getInputParameter(name);
        return value != null ? ((Number) value).intValue() : defaultValue;
    }

    protected Long readLongInput(String name, long defaultValue) {
        Object value = getInputParameter(name);
        return value != null ? ((Number) value).longValue() : defaultValue;
    }

    Map<String, Object> getOutputs() {
        return getOutputParameters();
    }
}
