package com.bonitasoft.connectors.kafka;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.bonitasoft.connectors.kafka.model.EventPublishResult;
import org.bonitasoft.engine.connector.ConnectorValidationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashMap;
import java.util.Map;

@ExtendWith(MockitoExtension.class)
class EventPublishJsonConnectorTest {

    @Mock
    private KafkaClient mockClient;

    private EventPublishJsonConnector connector;

    @BeforeEach
    void setUp() {
        connector = new EventPublishJsonConnector();
    }

    private Map<String, Object> validInputs() {
        var inputs = new HashMap<String, Object>();
        inputs.put("brokerType", "kafka");
        inputs.put("bootstrapServers", "localhost:9092");
        inputs.put("destination", "events-topic");
        inputs.put("jsonPayload", "{\"type\":\"order.created\"}");
        return inputs;
    }

    private void injectMockClient() throws Exception {
        var clientField = AbstractKafkaConnector.class.getDeclaredField("client");
        clientField.setAccessible(true);
        clientField.set(connector, mockClient);
    }

    @Test
    void shouldPublishEventSuccessfully() throws Exception {
        connector.setInputParameters(validInputs());
        connector.validateInputParameters();
        injectMockClient();
        when(mockClient.eventPublishJson(anyString(), anyString(), anyString()))
                .thenReturn(new EventPublishResult("kafka", "events-topic", true, 1700000000L));
        connector.executeBusinessLogic();
        assertThat(connector.getOutputs().get("success")).isEqualTo(true);
        assertThat(connector.getOutputs().get("brokerTypeOut")).isEqualTo("kafka");
        assertThat(connector.getOutputs().get("sent")).isEqualTo(true);
    }

    @Test
    void shouldFailWhenBrokerTypeMissing() {
        var inputs = validInputs();
        inputs.remove("brokerType");
        connector.setInputParameters(inputs);
        assertThatThrownBy(() -> connector.validateInputParameters())
                .isInstanceOf(ConnectorValidationException.class);
    }

    @Test
    void shouldFailWhenDestinationMissing() {
        var inputs = validInputs();
        inputs.remove("destination");
        connector.setInputParameters(inputs);
        assertThatThrownBy(() -> connector.validateInputParameters())
                .isInstanceOf(ConnectorValidationException.class);
    }

    @Test
    void shouldFailWhenJsonPayloadMissing() {
        var inputs = validInputs();
        inputs.remove("jsonPayload");
        connector.setInputParameters(inputs);
        assertThatThrownBy(() -> connector.validateInputParameters())
                .isInstanceOf(ConnectorValidationException.class);
    }

    @Test
    void shouldSetErrorOutputsOnFailure() throws Exception {
        connector.setInputParameters(validInputs());
        connector.validateInputParameters();
        injectMockClient();
        when(mockClient.eventPublishJson(anyString(), anyString(), anyString()))
                .thenThrow(new KafkaException("Invalid JSON payload"));
        connector.executeBusinessLogic();
        assertThat(connector.getOutputs().get("success")).isEqualTo(false);
    }
}
