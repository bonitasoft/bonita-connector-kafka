package com.bonitasoft.connectors.kafka;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.bonitasoft.connectors.kafka.model.ProduceMessageResult;
import org.bonitasoft.engine.connector.ConnectorValidationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashMap;
import java.util.Map;

@ExtendWith(MockitoExtension.class)
class RabbitmqPublishMessageConnectorTest {

    @Mock
    private KafkaClient mockClient;

    private RabbitmqPublishMessageConnector connector;

    @BeforeEach
    void setUp() {
        connector = new RabbitmqPublishMessageConnector();
    }

    private Map<String, Object> validInputs() {
        var inputs = new HashMap<String, Object>();
        inputs.put("bootstrapServers", "localhost:5672");
        inputs.put("routingKey", "test.queue");
        inputs.put("messageBody", "{\"event\":\"test\"}");
        return inputs;
    }

    private void injectMockClient() throws Exception {
        var clientField = AbstractKafkaConnector.class.getDeclaredField("client");
        clientField.setAccessible(true);
        clientField.set(connector, mockClient);
    }

    @Test
    void shouldPublishMessageSuccessfully() throws Exception {
        connector.setInputParameters(validInputs());
        connector.validateInputParameters();
        injectMockClient();
        when(mockClient.rabbitmqPublishMessage(anyString(), anyString(), anyString(), anyString()))
                .thenReturn(new ProduceMessageResult("/test.queue", 0, -1L, 1700000000L, true));
        connector.executeBusinessLogic();
        assertThat(connector.getOutputs().get("success")).isEqualTo(true);
        assertThat(connector.getOutputs().get("destination")).isEqualTo("/test.queue");
    }

    @Test
    void shouldFailWhenRoutingKeyMissing() {
        var inputs = validInputs();
        inputs.remove("routingKey");
        connector.setInputParameters(inputs);
        assertThatThrownBy(() -> connector.validateInputParameters())
                .isInstanceOf(ConnectorValidationException.class);
    }

    @Test
    void shouldFailWhenMessageBodyMissing() {
        var inputs = validInputs();
        inputs.remove("messageBody");
        connector.setInputParameters(inputs);
        assertThatThrownBy(() -> connector.validateInputParameters())
                .isInstanceOf(ConnectorValidationException.class);
    }

    @Test
    void shouldSetErrorOutputsOnFailure() throws Exception {
        connector.setInputParameters(validInputs());
        connector.validateInputParameters();
        injectMockClient();
        when(mockClient.rabbitmqPublishMessage(anyString(), anyString(), anyString(), anyString()))
                .thenThrow(new KafkaException("Connection refused"));
        connector.executeBusinessLogic();
        assertThat(connector.getOutputs().get("success")).isEqualTo(false);
    }

    @Test
    void shouldApplyDefaultsForOptionalInputs() throws Exception {
        connector.setInputParameters(validInputs());
        connector.validateInputParameters();
        var configField = AbstractKafkaConnector.class.getDeclaredField("configuration");
        configField.setAccessible(true);
        var config = (KafkaConfiguration) configField.get(connector);
        assertThat(config.getRabbitmqVirtualHost()).isEqualTo("/");
        assertThat(config.isRabbitmqSsl()).isFalse();
        assertThat(config.getContentType()).isEqualTo("application/json");
    }
}
