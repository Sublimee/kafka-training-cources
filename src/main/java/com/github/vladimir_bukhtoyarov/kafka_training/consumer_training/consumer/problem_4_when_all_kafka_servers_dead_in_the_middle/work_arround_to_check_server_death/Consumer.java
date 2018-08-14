package com.github.vladimir_bukhtoyarov.kafka_training.consumer_training.consumer.problem_4_when_all_kafka_servers_dead_in_the_middle.work_arround_to_check_server_death;

import com.github.vladimir_bukhtoyarov.kafka_training.consumer_training.util.Constants;
import com.github.vladimir_bukhtoyarov.kafka_training.consumer_training.util.JsonSerDer;
import com.github.vladimir_bukhtoyarov.kafka_training.consumer_training.util.Message;
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
    public static final long POLL_TIMEOUT = 1000L;
    public static final long POLL_THREASHOLD = POLL_TIMEOUT * 3;

    private final KafkaConsumer<String, Message> consumer;
    private final Thread thread = new Thread(this::consumeInfinitely, "Kafka-consumer-event-loop");
    private final Set<String> topics;

    private volatile List<String> unassignedTopics;
    private volatile String lastPollError;
    private volatile long lastPollStartedTimestamp;

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
        logger.info("Consumer started");
        try {
            while (true) {
                pollAndProcess();
            }
        } catch (WakeupException e) {
            logger.info("Consumer is going to stop normally");
        } catch (Throwable t) {
            logger.error("Consumer is going to stop", t);
        } finally {
            consumer.close();
        }
    }

    private void pollAndProcess() {
        ConsumerRecords<String, Message> records;
        try {
            lastPollStartedTimestamp = System.currentTimeMillis();
            records = consumer.poll(POLL_TIMEOUT);
            this.lastPollError = null;
        } catch (Throwable t) {
            logger.error("Failed to poll messages", t);
            this.lastPollError = "Last poll failed with error " + t.getMessage();
            return;
        }

        for (ConsumerRecord<String, Message> record : records) {
            try {
                processRecord(record);
            } catch (Throwable t) {
                logger.error("Failed to process record {}", record, t);
            }
        }

        try {
            consumer.commitSync();
        } catch (Throwable t) {
            logger.error("Failed to commit messages", t);
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
        StringBuilder msgBuilder = new StringBuilder();
        boolean healthy = true;

        // check consumer thread
        if (thread.isAlive()) {
            msgBuilder.append("Consumer thread " + thread.getName() + " is alive");
        } else {
            return new HealthStatus(false, "Consumer thread " + thread.getName() + " is dead");
        }

        // check assignment
        msgBuilder.append(" /");
        List<String> unassignedTopics = this.unassignedTopics;
        if (unassignedTopics.isEmpty()) {
            msgBuilder.append("Consumer assigned to all topics " + topics);
        } else {
            healthy = false;
            msgBuilder.append("Consumer not assigned to topics " + unassignedTopics);
        }

        // check success of last poll
        msgBuilder.append(" /");
        String lastPollError = this.lastPollError;
        long lastPollStartedTimestamp = this.lastPollStartedTimestamp;
        if (lastPollStartedTimestamp == 0) {
            healthy = false;
            msgBuilder.append("Poll never started.");
        } else if (lastPollError != null) {
            healthy = false;
            msgBuilder.append("Last poll failed with error " + lastPollError);
        } else if (System.currentTimeMillis() - lastPollStartedTimestamp > POLL_THREASHOLD) {
            healthy = false;
            long inProgressMillis = System.currentTimeMillis() - lastPollStartedTimestamp;
            msgBuilder.append("Poll hanged for " + inProgressMillis + " milliseconds");
        } else {
            msgBuilder.append("Last poll finished successfully.");
        }

        return new HealthStatus(healthy, msgBuilder.toString());
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
