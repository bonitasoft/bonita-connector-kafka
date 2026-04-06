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
class KafkaConsumeMessageConnectorTest {

    @Mock
    private KafkaClient mockClient;

    private KafkaConsumeMessageConnector connector;

    @BeforeEach
    void setUp() {
        connector = new KafkaConsumeMessageConnector();
    }

    private Map<String, Object> validInputs() {
        var inputs = new HashMap<String, Object>();
        inputs.put("bootstrapServers", "localhost:9092");
        inputs.put("topic", "test-topic");
        inputs.put("consumerGroup", "test-group");
        return inputs;
    }

    private void injectMockClient() throws Exception {
        var clientField = AbstractKafkaConnector.class.getDeclaredField("client");
        clientField.setAccessible(true);
        clientField.set(connector, mockClient);
    }

    @Test
    void shouldConsumeMessagesSuccessfully() throws Exception {
        connector.setInputParameters(validInputs());
        connector.validateInputParameters();
        injectMockClient();
        when(mockClient.kafkaConsumeMessage(anyString(), anyString(), anyLong(), anyInt()))
                .thenReturn(new ConsumeMessageResult(2, "[{\"key\":\"k1\"},{\"key\":\"k2\"}]", "test-topic", "test-group"));
        connector.executeBusinessLogic();
        assertThat(connector.getOutputs().get("success")).isEqualTo(true);
        assertThat(connector.getOutputs().get("messageCount")).isEqualTo(2);
        assertThat(connector.getOutputs().get("source")).isEqualTo("test-topic");
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
    void shouldFailWhenConsumerGroupMissing() {
        var inputs = validInputs();
        inputs.remove("consumerGroup");
        connector.setInputParameters(inputs);
        assertThatThrownBy(() -> connector.validateInputParameters())
                .isInstanceOf(ConnectorValidationException.class);
    }

    @Test
    void shouldSetErrorOutputsOnFailure() throws Exception {
        connector.setInputParameters(validInputs());
        connector.validateInputParameters();
        injectMockClient();
        when(mockClient.kafkaConsumeMessage(anyString(), anyString(), anyLong(), anyInt()))
                .thenThrow(new KafkaException("Timeout"));
        connector.executeBusinessLogic();
        assertThat(connector.getOutputs().get("success")).isEqualTo(false);
        assertThat((String) connector.getOutputs().get("errorMessage")).contains("Timeout");
    }

    @Test
    void shouldApplyDefaultsForOptionalInputs() throws Exception {
        connector.setInputParameters(validInputs());
        connector.validateInputParameters();
        var configField = AbstractKafkaConnector.class.getDeclaredField("configuration");
        configField.setAccessible(true);
        var config = (KafkaConfiguration) configField.get(connector);
        assertThat(config.getPollDurationMs()).isEqualTo(5000L);
        assertThat(config.getMaxRecords()).isEqualTo(10);
        assertThat(config.getAutoOffsetReset()).isEqualTo("earliest");
    }
}
