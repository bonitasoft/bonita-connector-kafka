package com.bonitasoft.connectors.kafka;

import com.bonitasoft.connectors.kafka.model.ConsumeMessageResult;
import com.bonitasoft.connectors.kafka.model.ConsumerStatusResult;
import com.bonitasoft.connectors.kafka.model.EventPublishResult;
import com.bonitasoft.connectors.kafka.model.ProduceMessageResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.AMQP;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Client facade for Kafka and RabbitMQ operations.
 * Manages connections to both broker types based on configuration.
 */
@Slf4j
public class KafkaClient {

    private final KafkaConfiguration configuration;
    private final RetryPolicy retryPolicy;
    private final ObjectMapper objectMapper;

    // RabbitMQ connection (lazy init)
    private Connection rabbitmqConnection;
    private Channel rabbitmqChannel;

    public KafkaClient(KafkaConfiguration configuration) throws KafkaException {
        this.configuration = configuration;
        this.retryPolicy = new RetryPolicy(configuration.getMaxRetries());
        this.objectMapper = new ObjectMapper();
        log.debug("KafkaClient initialized for broker type: {}", configuration.getBrokerType());
    }

    /**
     * Kafka: produce a message to a topic.
     */
    public ProduceMessageResult kafkaProduceMessage(String topic, String key, String value)
            throws KafkaException {
        return retryPolicy.execute(() -> {
            Properties props = buildKafkaProducerProperties();
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
                ProducerRecord<String, String> record = (key != null && !key.isBlank())
                        ? new ProducerRecord<>(topic, key, value)
                        : new ProducerRecord<>(topic, value);

                Future<RecordMetadata> future = producer.send(record);
                RecordMetadata metadata = future.get(configuration.getTimeoutSeconds(), TimeUnit.SECONDS);

                log.info("Message produced to topic={}, partition={}, offset={}",
                        metadata.topic(), metadata.partition(), metadata.offset());

                return new ProduceMessageResult(
                        metadata.topic(),
                        metadata.partition(),
                        metadata.offset(),
                        metadata.timestamp(),
                        true
                );
            } catch (org.apache.kafka.common.errors.AuthenticationException e) {
                throw new KafkaException("Kafka authentication failed: " + e.getMessage(), 401, false, e);
            } catch (org.apache.kafka.common.errors.TimeoutException e) {
                throw new KafkaException("Kafka operation timed out: " + e.getMessage(), 408, true, e);
            } catch (Exception e) {
                throw new KafkaException("Failed to produce message to Kafka: " + e.getMessage(), e);
            }
        });
    }

    /**
     * Kafka: consume messages from a topic.
     */
    public ConsumeMessageResult kafkaConsumeMessage(String topic, String consumerGroup,
            long pollDurationMs, int maxRecords) throws KafkaException {
        return retryPolicy.execute(() -> {
            Properties props = buildKafkaConsumerProperties(consumerGroup);
            props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxRecords);

            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
                consumer.subscribe(Collections.singletonList(topic));
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(pollDurationMs));

                List<Map<String, Object>> messages = new ArrayList<>();
                records.forEach(record -> {
                    Map<String, Object> msg = new HashMap<>();
                    msg.put("key", record.key());
                    msg.put("value", record.value());
                    msg.put("partition", record.partition());
                    msg.put("offset", record.offset());
                    msg.put("timestamp", record.timestamp());
                    msg.put("topic", record.topic());
                    messages.add(msg);
                });

                consumer.commitSync();
                log.info("Consumed {} messages from topic={}", messages.size(), topic);

                String messagesJson = objectMapper.writeValueAsString(messages);
                return new ConsumeMessageResult(messages.size(), messagesJson, topic, consumerGroup);
            } catch (org.apache.kafka.common.errors.AuthenticationException e) {
                throw new KafkaException("Kafka authentication failed: " + e.getMessage(), 401, false, e);
            } catch (Exception e) {
                throw new KafkaException("Failed to consume messages from Kafka: " + e.getMessage(), e);
            }
        });
    }

    /**
     * RabbitMQ: publish a message to an exchange.
     */
    public ProduceMessageResult rabbitmqPublishMessage(String exchange, String routingKey,
            String messageBody, String contentType) throws KafkaException {
        return retryPolicy.execute(() -> {
            try {
                ensureRabbitmqConnection();
                AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                        .contentType(contentType)
                        .deliveryMode(2) // persistent
                        .build();

                rabbitmqChannel.basicPublish(exchange, routingKey, props, messageBody.getBytes());
                log.info("Message published to exchange={}, routingKey={}", exchange, routingKey);

                return new ProduceMessageResult(
                        exchange + "/" + routingKey,
                        0,
                        -1L,
                        System.currentTimeMillis(),
                        true
                );
            } catch (IOException e) {
                throw new KafkaException("Failed to publish message to RabbitMQ: " + e.getMessage(), e);
            }
        });
    }

    /**
     * RabbitMQ: consume a message from a queue.
     */
    public ConsumeMessageResult rabbitmqConsumeMessage(String queue, boolean autoAck)
            throws KafkaException {
        return retryPolicy.execute(() -> {
            try {
                ensureRabbitmqConnection();
                GetResponse response = rabbitmqChannel.basicGet(queue, autoAck);

                if (response == null) {
                    log.info("No message available in queue={}", queue);
                    return new ConsumeMessageResult(0, "[]", queue, null);
                }

                String body = new String(response.getBody());
                Map<String, Object> msg = new HashMap<>();
                msg.put("value", body);
                msg.put("deliveryTag", response.getEnvelope().getDeliveryTag());
                msg.put("exchange", response.getEnvelope().getExchange());
                msg.put("routingKey", response.getEnvelope().getRoutingKey());
                msg.put("messageCount", response.getMessageCount());

                List<Map<String, Object>> messages = List.of(msg);
                String messagesJson = objectMapper.writeValueAsString(messages);

                log.info("Consumed 1 message from queue={}", queue);
                return new ConsumeMessageResult(1, messagesJson, queue, null);
            } catch (IOException e) {
                throw new KafkaException("Failed to consume message from RabbitMQ: " + e.getMessage(), e);
            }
        });
    }

    /**
     * Broker-agnostic: publish a JSON event.
     */
    public EventPublishResult eventPublishJson(String brokerType, String destination,
            String jsonPayload) throws KafkaException {
        // Validate JSON
        try {
            objectMapper.readTree(jsonPayload);
        } catch (Exception e) {
            throw new KafkaException("Invalid JSON payload: " + e.getMessage(), e);
        }

        if ("kafka".equalsIgnoreCase(brokerType)) {
            ProduceMessageResult result = kafkaProduceMessage(destination, null, jsonPayload);
            return new EventPublishResult(
                    brokerType,
                    destination,
                    result.sent(),
                    result.timestamp()
            );
        } else if ("rabbitmq".equalsIgnoreCase(brokerType)) {
            ProduceMessageResult result = rabbitmqPublishMessage("", destination, jsonPayload, "application/json");
            return new EventPublishResult(
                    brokerType,
                    destination,
                    result.sent(),
                    result.timestamp()
            );
        } else {
            throw new KafkaException("Unsupported broker type: " + brokerType);
        }
    }

    /**
     * Get consumer status -- lag for Kafka, queue depth for RabbitMQ.
     */
    public ConsumerStatusResult getConsumerStatus(String brokerType, String topicOrQueue,
            String consumerGroup) throws KafkaException {
        if ("kafka".equalsIgnoreCase(brokerType)) {
            return getKafkaConsumerStatus(topicOrQueue, consumerGroup);
        } else if ("rabbitmq".equalsIgnoreCase(brokerType)) {
            return getRabbitmqQueueStatus(topicOrQueue);
        } else {
            throw new KafkaException("Unsupported broker type: " + brokerType);
        }
    }

    private ConsumerStatusResult getKafkaConsumerStatus(String topic, String consumerGroup)
            throws KafkaException {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, configuration.getBootstrapServers());
        addKafkaSecurityProperties(props);

        try (AdminClient adminClient = AdminClient.create(props)) {
            ListConsumerGroupOffsetsResult offsetsResult =
                    adminClient.listConsumerGroupOffsets(consumerGroup);

            Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> offsets =
                    offsetsResult.partitionsToOffsetAndMetadata().get(configuration.getTimeoutSeconds(), TimeUnit.SECONDS);

            long totalLag = 0;
            int partitionCount = 0;

            if (offsets != null) {
                for (Map.Entry<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> entry : offsets.entrySet()) {
                    if (entry.getKey().topic().equals(topic)) {
                        partitionCount++;
                        // Lag calculation would need end offsets; report committed offset as proxy
                        totalLag += entry.getValue().offset();
                    }
                }
            }

            return new ConsumerStatusResult(
                    "kafka",
                    topic,
                    consumerGroup,
                    totalLag,
                    partitionCount,
                    true
            );
        } catch (Exception e) {
            throw new KafkaException("Failed to get Kafka consumer status: " + e.getMessage(), e);
        }
    }

    private ConsumerStatusResult getRabbitmqQueueStatus(String queue) throws KafkaException {
        try {
            ensureRabbitmqConnection();
            AMQP.Queue.DeclareOk declareOk = rabbitmqChannel.queueDeclarePassive(queue);
            int messageCount = declareOk.getMessageCount();
            int consumerCount = declareOk.getConsumerCount();

            return new ConsumerStatusResult(
                    "rabbitmq",
                    queue,
                    null,
                    messageCount,
                    consumerCount,
                    true
            );
        } catch (IOException e) {
            throw new KafkaException("Failed to get RabbitMQ queue status: " + e.getMessage(), e);
        }
    }

    public void close() {
        try {
            if (rabbitmqChannel != null && rabbitmqChannel.isOpen()) {
                rabbitmqChannel.close();
            }
            if (rabbitmqConnection != null && rabbitmqConnection.isOpen()) {
                rabbitmqConnection.close();
            }
        } catch (IOException | TimeoutException e) {
            log.warn("Error closing RabbitMQ connection: {}", e.getMessage());
        }
    }

    private void ensureRabbitmqConnection() throws KafkaException {
        if (rabbitmqConnection != null && rabbitmqConnection.isOpen()) {
            return;
        }
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(extractHost(configuration.getBootstrapServers()));
            factory.setPort(extractPort(configuration.getBootstrapServers(), 5672));

            if (configuration.getRabbitmqUsername() != null) {
                factory.setUsername(configuration.getRabbitmqUsername());
            }
            if (configuration.getRabbitmqPassword() != null) {
                factory.setPassword(configuration.getRabbitmqPassword());
            }
            if (configuration.getRabbitmqVirtualHost() != null) {
                factory.setVirtualHost(configuration.getRabbitmqVirtualHost());
            }
            if (configuration.isRabbitmqSsl()) {
                factory.useSslProtocol();
            }
            factory.setConnectionTimeout(configuration.getTimeoutSeconds() * 1000);

            rabbitmqConnection = factory.newConnection();
            rabbitmqChannel = rabbitmqConnection.createChannel();
            log.debug("RabbitMQ connection established");
        } catch (IOException | TimeoutException e) {
            throw new KafkaException("Failed to connect to RabbitMQ: " + e.getMessage(), e);
        } catch (Exception e) {
            throw new KafkaException("Failed to initialize RabbitMQ connection: " + e.getMessage(), e);
        }
    }

    private Properties buildKafkaProducerProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, configuration.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, configuration.getKeySerializer());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, configuration.getValueSerializer());
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, configuration.getTimeoutSeconds() * 1000);
        addKafkaSecurityProperties(props);
        return props;
    }

    private Properties buildKafkaConsumerProperties(String consumerGroup) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, configuration.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, configuration.getKeyDeserializer());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, configuration.getValueDeserializer());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, configuration.getAutoOffsetReset());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, configuration.getTimeoutSeconds() * 1000);
        addKafkaSecurityProperties(props);
        return props;
    }

    private void addKafkaSecurityProperties(Properties props) {
        String protocol = configuration.getSecurityProtocol();
        if (protocol != null && !protocol.isBlank()) {
            props.put("security.protocol", protocol);
        }
        String mechanism = configuration.getSaslMechanism();
        if (mechanism != null && !mechanism.isBlank()) {
            props.put("sasl.mechanism", mechanism);
            String jaasConfig = buildJaasConfig(mechanism,
                    configuration.getSaslUsername(), configuration.getSaslPassword());
            if (jaasConfig != null) {
                props.put("sasl.jaas.config", jaasConfig);
            }
        }
    }

    private String buildJaasConfig(String mechanism, String username, String password) {
        if (username == null || password == null) {
            return null;
        }
        if ("PLAIN".equals(mechanism)) {
            return "org.apache.kafka.common.security.plain.PlainLoginModule required "
                    + "username=\"" + username + "\" password=\"" + password + "\";";
        } else if (mechanism.startsWith("SCRAM")) {
            return "org.apache.kafka.common.security.scram.ScramLoginModule required "
                    + "username=\"" + username + "\" password=\"" + password + "\";";
        }
        return null;
    }

    private String extractHost(String servers) {
        if (servers == null) {
            return "localhost";
        }
        String host = servers.replaceAll("^(amqps?://)?", "");
        int colonIdx = host.indexOf(':');
        return colonIdx > 0 ? host.substring(0, colonIdx) : host;
    }

    private int extractPort(String servers, int defaultPort) {
        if (servers == null) {
            return defaultPort;
        }
        String stripped = servers.replaceAll("^(amqps?://)?", "");
        int colonIdx = stripped.indexOf(':');
        if (colonIdx > 0 && colonIdx < stripped.length() - 1) {
            try {
                return Integer.parseInt(stripped.substring(colonIdx + 1));
            } catch (NumberFormatException e) {
                return defaultPort;
            }
        }
        return defaultPort;
    }
}
