package com.bonitasoft.connectors.kafka;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.bonitasoft.connectors.kafka.model.ConsumeMessageResult;
import org.bonitasoft.engine.connector.ConnectorValidationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashMap;
import java.util.Map;

@ExtendWith(MockitoExtension.class)
class RabbitmqConsumeMessageConnectorTest {

    @Mock
    private KafkaClient mockClient;

    private RabbitmqConsumeMessageConnector connector;

    @BeforeEach
    void setUp() {
        connector = new RabbitmqConsumeMessageConnector();
    }

    private Map<String, Object> validInputs() {
        var inputs = new HashMap<String, Object>();
        inputs.put("bootstrapServers", "localhost:5672");
        inputs.put("queue", "test-queue");
        return inputs;
    }

    private void injectMockClient() throws Exception {
        var clientField = AbstractKafkaConnector.class.getDeclaredField("client");
        clientField.setAccessible(true);
        clientField.set(connector, mockClient);
    }

    @Test
    void shouldConsumeMessageSuccessfully() throws Exception {
        connector.setInputParameters(validInputs());
        connector.validateInputParameters();
        injectMockClient();
        when(mockClient.rabbitmqConsumeMessage(anyString(), anyBoolean()))
                .thenReturn(new ConsumeMessageResult(1, "[{\"value\":\"hello\"}]", "test-queue", null));
        connector.executeBusinessLogic();
        assertThat(connector.getOutputs().get("success")).isEqualTo(true);
        assertThat(connector.getOutputs().get("messageCount")).isEqualTo(1);
        assertThat(connector.getOutputs().get("source")).isEqualTo("test-queue");
    }

    @Test
    void shouldFailWhenQueueMissing() {
        var inputs = validInputs();
        inputs.remove("queue");
        connector.setInputParameters(inputs);
        assertThatThrownBy(() -> connector.validateInputParameters())
                .isInstanceOf(ConnectorValidationException.class);
    }

    @Test
    void shouldSetErrorOutputsOnFailure() throws Exception {
        connector.setInputParameters(validInputs());
        connector.validateInputParameters();
        injectMockClient();
        when(mockClient.rabbitmqConsumeMessage(anyString(), anyBoolean()))
                .thenThrow(new KafkaException("Queue not found"));
        connector.executeBusinessLogic();
        assertThat(connector.getOutputs().get("success")).isEqualTo(false);
        assertThat((String) connector.getOutputs().get("errorMessage")).contains("Queue not found");
    }

    @Test
    void shouldApplyDefaultsForOptionalInputs() throws Exception {
        connector.setInputParameters(validInputs());
        connector.validateInputParameters();
        var configField = AbstractKafkaConnector.class.getDeclaredField("configuration");
        configField.setAccessible(true);
        var config = (KafkaConfiguration) configField.get(connector);
        assertThat(config.isAutoAck()).isTrue();
        assertThat(config.getRabbitmqVirtualHost()).isEqualTo("/");
    }
}
