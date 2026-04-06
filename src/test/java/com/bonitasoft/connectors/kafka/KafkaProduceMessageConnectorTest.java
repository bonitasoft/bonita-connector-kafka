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
class KafkaProduceMessageConnectorTest {

    @Mock
    private KafkaClient mockClient;

    private KafkaProduceMessageConnector connector;

    @BeforeEach
    void setUp() {
        connector = new KafkaProduceMessageConnector();
    }

    private Map<String, Object> validInputs() {
        var inputs = new HashMap<String, Object>();
        inputs.put("bootstrapServers", "localhost:9092");
        inputs.put("topic", "test-topic");
        inputs.put("messageValue", "{\"event\":\"test\"}");
        return inputs;
    }

    private void injectMockClient() throws Exception {
        var clientField = AbstractKafkaConnector.class.getDeclaredField("client");
        clientField.setAccessible(true);
        clientField.set(connector, mockClient);
    }

    @Test
    void shouldProduceMessageSuccessfully() throws Exception {
        connector.setInputParameters(validInputs());
        connector.validateInputParameters();
        injectMockClient();
        when(mockClient.kafkaProduceMessage(anyString(), any(), anyString()))
                .thenReturn(new ProduceMessageResult("test-topic", 0, 42L, 1700000000L, true));
        connector.executeBusinessLogic();
        assertThat(connector.getOutputs().get("success")).isEqualTo(true);
        assertThat(connector.getOutputs().get("destination")).isEqualTo("test-topic");
        assertThat(connector.getOutputs().get("partition")).isEqualTo(0);
        assertThat(connector.getOutputs().get("offset")).isEqualTo(42L);
    }

    @Test
    void shouldFailWhenBootstrapServersMissing() {
        var inputs = validInputs();
        inputs.remove("bootstrapServers");
        connector.setInputParameters(inputs);
        assertThatThrownBy(() -> connector.validateInputParameters())
                .isInstanceOf(ConnectorValidationException.class);
    }

    @Test
    void shouldFailWhenTopicMissing() {
        var inputs = validInputs();
        inputs.remove("topic");
        connector.setInputParameters(inputs);
        assertThatThrownBy(() -> connector.validateInputParameters())
                .isInstanceOf(ConnectorValidationException.class);
    }

    @Test
    void shouldFailWhenMessageValueMissing() {
        var inputs = validInputs();
        inputs.remove("messageValue");
        connector.setInputParameters(inputs);
        assertThatThrownBy(() -> connector.validateInputParameters())
                .isInstanceOf(ConnectorValidationException.class);
    }

    @Test
    void shouldSetErrorOutputsOnFailure() throws Exception {
        connector.setInputParameters(validInputs());
        connector.validateInputParameters();
        injectMockClient();
        when(mockClient.kafkaProduceMessage(anyString(), any(), anyString()))
                .thenThrow(new KafkaException("Connection refused"));
        connector.executeBusinessLogic();
        assertThat(connector.getOutputs().get("success")).isEqualTo(false);
        assertThat((String) connector.getOutputs().get("errorMessage")).contains("Connection refused");
    }

    @Test
    void shouldApplyDefaultsForOptionalInputs() throws Exception {
        connector.setInputParameters(validInputs());
        connector.validateInputParameters();
        var configField = AbstractKafkaConnector.class.getDeclaredField("configuration");
        configField.setAccessible(true);
        var config = (KafkaConfiguration) configField.get(connector);
        assertThat(config.getSecurityProtocol()).isEqualTo("PLAINTEXT");
        assertThat(config.getTimeoutSeconds()).isEqualTo(30);
        assertThat(config.getMaxRetries()).isEqualTo(3);
    }
}
