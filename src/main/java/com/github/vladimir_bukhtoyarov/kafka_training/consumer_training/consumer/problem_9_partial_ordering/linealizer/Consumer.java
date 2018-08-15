package com.github.vladimir_bukhtoyarov.kafka_training.consumer_training.consumer.problem_9_partial_ordering.linealizer;

import com.github.vladimir_bukhtoyarov.kafka_training.consumer_training.util.Constants;
import com.github.vladimir_bukhtoyarov.kafka_training.consumer_training.util.JsonSerDer;
import com.github.vladimir_bukhtoyarov.kafka_training.consumer_training.util.Message;
import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.Bucket;
import io.github.bucket4j.Bucket4j;
import io.github.bucket4j.local.SynchronizationStrategy;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public class Consumer {

    private static final Logger logger = LoggerFactory.getLogger(Consumer.class);
    public static final long POLL_TIMEOUT = 1000L;
    public static final long POLL_THREASHOLD = POLL_TIMEOUT * 3;
    public static final long LAG_THREASHOLD = 1000;
    private static final long PREFETCH_THRESHOLD = 1000;

    private final KafkaConsumer<String, Message> consumer;
    private final Thread thread = new Thread(this::consumeInfinitely, "Kafka-consumer-event-loop");
    private final Set<String> topics;
    private final ThreadPoolExecutor executor;
    private final Linealizer<String, Void> linealizer = new Linealizer<>();

    private volatile List<String> unassignedTopics;
    private volatile String lastPollError;
    private volatile long lastPollStartedTimestamp;
    private volatile long lag = 0;
    private volatile long inProgressRecordsCount = 0;
    private volatile boolean paused = false;

    private final Map<TopicPartition, Deque<RecordInProgress>> recordsInProgress = new HashMap<>();

    private Bucket updateLagThrottler = Bucket4j.builder()
                                    .addLimit(Bandwidth.simple(1, Duration.ofSeconds(10)))
                                    .withSynchronizationStrategy(SynchronizationStrategy.NONE)
                                    .build();

    public Consumer(String clientId, Set<String> topics, ThreadPoolExecutor executor) {
        this(clientId, topics, executor, Collections.emptyMap());
    }

    public Consumer(String clientId, Set<String> topics, ThreadPoolExecutor executor, Map<String, Object> propertiesOverride) {
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
        this.executor = executor;

        consumer.subscribe(topics, new RebalanceListener());
    }

    public void start() {
        thread.start();
    }

    public void shutdown() {
        consumer.pause(consumer.assignment());
        waitAllMessageToBeProcessedAndCommit();
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

        // schedule async task for all incoming message
        for (ConsumerRecord<String, Message> record : records) {
            CompletableFuture<Void> feature;
            try {
                feature = scheduleRecord(record);
            } catch (Throwable t) {
                feature = new CompletableFuture<>();
                feature.completeExceptionally(t);
                logger.error("Failed to schedule record {}", record, t);
            }
            RecordInProgress recordInProgress = new RecordInProgress(record, feature);
            TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
            Deque<RecordInProgress> partitionInProgressRecords = recordsInProgress.computeIfAbsent(topicPartition, key -> new LinkedList<>());
            partitionInProgressRecords.add(recordInProgress);
        }

        asyncCommitProcessedMessages();
        toggleConsumption();

        try {
            updateLag();
        } catch (Throwable t) {
            logger.error("Failed to update lag statistics", t);
        }

    }

    private void asyncCommitProcessedMessages() {
        int messagesToCommitCount = 0;
        Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();

        for (Map.Entry<TopicPartition, Deque<RecordInProgress>> partitionEntry : recordsInProgress.entrySet()) {
            TopicPartition topicPartition = partitionEntry.getKey();
            Deque<RecordInProgress> partitionRecords = partitionEntry.getValue();
            while (true) {
                RecordInProgress recordInProgress = partitionRecords.peekFirst();
                if (recordInProgress == null || !recordInProgress.getResult().isDone()) {
                    break;
                }
                messagesToCommitCount++;
                OffsetAndMetadata offsetToCommit = new OffsetAndMetadata(recordInProgress.getRecord().offset() + 1);
                offsetsToCommit.put(topicPartition, offsetToCommit);
                partitionRecords.removeFirst();
            }
        }

        if (offsetsToCommit.isEmpty() ) {
            return;
        }

        logger.info("Going to commit {} messages", messagesToCommitCount);
        OffsetCommitCallback callback = new AsyncOffsetCommitCallback(offsetsToCommit, messagesToCommitCount);
        consumer.commitAsync(offsetsToCommit, callback);
    }

    private void updateLag() {
        if (!updateLagThrottler.tryConsume(1)) {
            return;
        }
        Set<TopicPartition> assignment = consumer.assignment();
        Map<TopicPartition, Long> endOffsets = consumer.endOffsets(assignment);

        long lag = 0;
        for (TopicPartition assignedPartiotion: assignment) {
            long position = consumer.position(assignedPartiotion);
            Long endOffset = endOffsets.get(assignedPartiotion);
            if (endOffset == null) {
                continue;
            }
            long partitionLag = endOffset - position;
            if (partitionLag > 0) {
                lag += partitionLag;
            }
        }
        this.lag = lag;
    }

    private CompletableFuture<Void> scheduleRecord(ConsumerRecord<String, Message> record) {
        inProgressRecordsCount++;
        Linealizer.LinearizedTask task = linealizer.processOnKey(record.key(), executor, () -> {
            processRecord(record);
            return null;
        });
        return task.getFuture();
    }

    private void processRecord(ConsumerRecord<String, Message> record) {
        String key = record.key();
        Message payload = record.value();
        logger.info("Received partition={} offset={} key={} payload={}", record.partition(), record.offset(), key, payload);

        if (payload.getDelayMillis() != null) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(payload.getDelayMillis()));
        }
        if (payload.getErrorClass() == null) {
            logger.info("Processed partition={} offset={} key={} payload={}", record.partition(), record.offset(), key, payload);
            return;
        }
        Throwable t;
        try {
            Class errorClass = Class.forName(payload.getErrorClass());
            t = (Throwable) errorClass.newInstance();
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
        throw new RuntimeException(t);
    }

    private void toggleConsumption() {
        long inProgressRecordsCount = this.inProgressRecordsCount;

        if (!paused) {
            if (inProgressRecordsCount > PREFETCH_THRESHOLD) {
                consumer.pause(consumer.assignment());
                paused = true;
                logger.warn("Subscriptions paused because of over consumption, threshold is {} messages, {} messages is in progress", PREFETCH_THRESHOLD, inProgressRecordsCount);
            }
            return;
        } else {
            if (inProgressRecordsCount <= PREFETCH_THRESHOLD) {
                consumer.resume(consumer.paused());
                logger.warn("Resume subscriptions after pause threshold is {} messages, {} messages is in progress", PREFETCH_THRESHOLD, inProgressRecordsCount);
                paused = false;
            }
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

        // check the lag
        msgBuilder.append(" /");
        long lag = this.lag;
        if (lag > LAG_THREASHOLD) {
            healthy = false;
            msgBuilder.append("Consumer overloaded. Lag is " + lag + " messages");
        } else {
            msgBuilder.append("Lag is " + lag + " messages");
        }

        // check overloading
        msgBuilder.append(" /");
        long inProgressRecordsCount = this.inProgressRecordsCount;
        if (inProgressRecordsCount > PREFETCH_THRESHOLD) {
            healthy = false;
            msgBuilder.append("Consumer overloaded. There are  " + inProgressRecordsCount + " messages in progress");
        } else {
            msgBuilder.append("Consumer is not overloaded");
        }

        return new HealthStatus(healthy, msgBuilder.toString());
    }

    private class AsyncOffsetCommitCallback implements OffsetCommitCallback {

        private final Map<TopicPartition, OffsetAndMetadata> offsetsToCommit;
        private final int messagesToCommit;

        private AsyncOffsetCommitCallback(Map<TopicPartition, OffsetAndMetadata> offsetsToCommit, int messagesToCommit) {
            this.offsetsToCommit = offsetsToCommit;
            this.messagesToCommit = messagesToCommit;
        }

        @Override
        public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
            inProgressRecordsCount -= messagesToCommit;

            // independent from result we issue permits to consume more massages from broker
            if (exception == null) {
                logger.info("Successfully committed {} messages", messagesToCommit);
            } else {
                logger.error("Fail to commit partitions {}, the {} messages can be duplicated", offsetsToCommit, messagesToCommit, exception);
            }
        }
    }

    private void waitAllMessageToBeProcessedAndCommit() {
        // cancelAll all unscheduled tasks
        List<Runnable> unscheduledTasks = new ArrayList<>();
        executor.getQueue().drainTo(unscheduledTasks);
        for (Runnable cancelledTask : unscheduledTasks) {
            ((Linealizer.LinearizedTask) cancelledTask).cancelAll();
        }

        // wait uncompleted tasks
        int messagesToCommit = 0;
        Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
        for (Map.Entry<TopicPartition, Deque<RecordInProgress>> partitionEntry : recordsInProgress.entrySet()) {
            TopicPartition topicPartition = partitionEntry.getKey();
            Deque<RecordInProgress> partitionRecords = partitionEntry.getValue();

            while (true) {
                RecordInProgress recordInProgress = partitionRecords.peekFirst();
                if (recordInProgress == null || recordInProgress.getResult().isCancelled()) {
                    break;
                }
                CompletableFuture<Void> messageFuture = recordInProgress.getResult();
                while (!messageFuture.isDone()) {
                    try {
                        messageFuture.get();
                        break;
                    } catch (Throwable t) {
                        logger.error("Failed to wait feature result", t);
                    }
                }
                messagesToCommit++;
                OffsetAndMetadata offsetToCommit = new OffsetAndMetadata(recordInProgress.getRecord().offset() + 1);
                offsetsToCommit.put(topicPartition, offsetToCommit);
                partitionRecords.removeFirst();
            }
        }

        inProgressRecordsCount = 0;
        if (messagesToCommit == 0) {
            logger.info("Nothing to commit");
            return;
        }

        logger.info("Going to commit {} messages", messagesToCommit);
        try {
            consumer.commitSync(offsetsToCommit);
        } catch (Throwable t) {
            logger.error("{} messages can be duplicated because of fail to commit", messagesToCommit, t);
        } finally {
            recordsInProgress.clear();
        }
    }

    private static class RecordInProgress {

        private final ConsumerRecord<String, Message> record;
        private final CompletableFuture<Void> result;

        public RecordInProgress(ConsumerRecord<String, Message> record, CompletableFuture<Void> result) {
            this.record = record;
            this.result = result;
        }

        public CompletableFuture<Void> getResult() {
            return result;
        }

        public ConsumerRecord<String, Message> getRecord() {
            return record;
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

            // need to wait for processing already taken messages in order to avoid duplicates
            waitAllMessageToBeProcessedAndCommit();
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

    public Linealizer<String, Void> getLinealizer() {
        return linealizer;
    }

}
