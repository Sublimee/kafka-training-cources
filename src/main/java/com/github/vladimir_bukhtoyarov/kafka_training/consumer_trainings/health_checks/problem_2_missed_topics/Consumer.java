package com.github.vladimir_bukhtoyarov.kafka_training.consumer_trainings.health_checks.problem_2_missed_topics;

import com.github.vladimir_bukhtoyarov.kafka_training.consumer_trainings.util.Constants;
import com.github.vladimir_bukhtoyarov.kafka_training.consumer_trainings.util.JsonSerDer;
import com.github.vladimir_bukhtoyarov.kafka_training.consumer_trainings.util.Message;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public class Consumer {

    private static final Logger logger = LoggerFactory.getLogger(Consumer.class);

    private final KafkaConsumer<String, Message> consumer;
    private final Thread thread = new Thread(this::consumeInfinitely, "Kafka-consumer-event-loop");
    private final Set<String> topics;

    private volatile List<String> unassignedTopics;

    public Consumer(String clientId, Set<String> topics) {
        this(clientId, topics, Collections.emptyMap());
    }

    public Consumer(String clientId, Set<String> topics, Map<String, Object> propertiesOverride) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.bootstrapServers);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, Constants.GROUP_ID);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);

        propertiesOverride.forEach(properties::put);

        this.consumer = new KafkaConsumer<>(properties, new StringDeserializer(), new JsonSerDer());
        this.topics = topics;
        this.unassignedTopics = new ArrayList<>(topics);

        consumer.subscribe(topics, new RebalanceListener());
    }

    public void start() {
        thread.start();
    }

    public void shutdown() {
        consumer.wakeup();
    }

    private void consumeInfinitely() {
        try {
            while (true) {
                ConsumerRecords<String, Message> records = consumer.poll(Long.MAX_VALUE);
                for (ConsumerRecord<String, Message> record : records) {
                    processRecord(record);
                }
                consumer.commitSync();
            }
        } catch (WakeupException e) {
            logger.info("Consumer is going to stop normally");
        } catch (Throwable t) {
            logger.error("Consumer is going to stop");
        } finally {
            consumer.close();
        }
    }

    private void processRecord(ConsumerRecord<String, Message> record) throws Throwable {
        String key = record.key();
        Message payload = record.value();
        logger.info("Received partition={} offset={} key={} payload={}", record.partition(), record.offset(), key, payload);

        if (payload.getDelayMillis() != null) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(payload.getDelayMillis()));
        }

        if (payload.getErrorClass() != null) {
            Class errorClass = Class.forName(payload.getErrorClass());
            Throwable t = (Throwable) errorClass.newInstance();
            throw t;
        }
    }

    public HealthStatus getHealth() {
        List<String> unassignedTopics = this.unassignedTopics;
        if (unassignedTopics.isEmpty()) {
            return new HealthStatus(true, "Consumer assigned to all topics " + topics);
        } else {
            return new HealthStatus(false, "Consumer not assigned to topics " + unassignedTopics);
        }
    }

    public static final class HealthStatus {

        private final boolean healthy;
        private final String message;

        public HealthStatus(boolean healthy, String message) {
            this.healthy = healthy;
            this.message = message;
        }

        public boolean isHealthy() {
            return healthy;
        }

        public String getMessage() {
            return message;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("HealthStatus{");
            sb.append("healthy=").append(healthy);
            sb.append(", message='").append(message).append('\'');
            sb.append('}');
            return sb.toString();
        }
    }

    private class RebalanceListener implements ConsumerRebalanceListener {
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            unassignedTopics = new ArrayList<>(topics);
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            List<String> unassignedTopics = new ArrayList<>(topics);
            for (TopicPartition topicPartition : partitions) {
                unassignedTopics.remove(topicPartition.topic());
                if (unassignedTopics.isEmpty()) {
                    break;
                }
            }
            Consumer.this.unassignedTopics = unassignedTopics;
        }
    }

}
