package com.bonitasoft.connectors.kafka;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.bonitasoft.connectors.kafka.model.ConsumerStatusResult;
import org.bonitasoft.engine.connector.ConnectorValidationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashMap;
import java.util.Map;

@ExtendWith(MockitoExtension.class)
class EventGetConsumerStatusConnectorTest {

    @Mock
    private KafkaClient mockClient;

    private EventGetConsumerStatusConnector connector;

    @BeforeEach
    void setUp() {
        connector = new EventGetConsumerStatusConnector();
    }

    private Map<String, Object> validInputs() {
        var inputs = new HashMap<String, Object>();
        inputs.put("brokerType", "kafka");
        inputs.put("bootstrapServers", "localhost:9092");
        inputs.put("topicOrQueue", "test-topic");
        inputs.put("consumerGroup", "test-group");
        return inputs;
    }

    private void injectMockClient() throws Exception {
        var clientField = AbstractKafkaConnector.class.getDeclaredField("client");
        clientField.setAccessible(true);
        clientField.set(connector, mockClient);
    }

    @Test
    void shouldGetConsumerStatusSuccessfully() throws Exception {
        connector.setInputParameters(validInputs());
        connector.validateInputParameters();
        injectMockClient();
        when(mockClient.getConsumerStatus(anyString(), anyString(), anyString()))
                .thenReturn(new ConsumerStatusResult("kafka", "test-topic", "test-group", 150L, 3, true));
        connector.executeBusinessLogic();
        assertThat(connector.getOutputs().get("success")).isEqualTo(true);
        assertThat(connector.getOutputs().get("brokerTypeOut")).isEqualTo("kafka");
        assertThat(connector.getOutputs().get("lagOrDepth")).isEqualTo(150L);
        assertThat(connector.getOutputs().get("connected")).isEqualTo(true);
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
    void shouldFailWhenTopicOrQueueMissing() {
        var inputs = validInputs();
        inputs.remove("topicOrQueue");
        connector.setInputParameters(inputs);
        assertThatThrownBy(() -> connector.validateInputParameters())
                .isInstanceOf(ConnectorValidationException.class);
    }

    @Test
    void shouldSetErrorOutputsOnFailure() throws Exception {
        connector.setInputParameters(validInputs());
        connector.validateInputParameters();
        injectMockClient();
        when(mockClient.getConsumerStatus(anyString(), anyString(), anyString()))
                .thenThrow(new KafkaException("Connection failed"));
        connector.executeBusinessLogic();
        assertThat(connector.getOutputs().get("success")).isEqualTo(false);
        assertThat((String) connector.getOutputs().get("errorMessage")).contains("Connection failed");
    }
}
