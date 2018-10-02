package com.github.vladimir_bukhtoyarov.kafka_training.consumer_trainings.util;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;

public class SeekToEnd {

    private static final Logger logger = LoggerFactory.getLogger(SeekToEnd.class);

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.bootstrapServers);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, Constants.GROUP_ID);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "reset-offset-tool");

        final KafkaConsumer<String, Message> consumer = new KafkaConsumer<>(properties, new StringDeserializer(), new JsonSerDer());
        consumer.subscribe(Collections.singleton(Constants.TOPIC));
        consumer.poll(100);
        Set<TopicPartition> assignment = consumer.assignment();
        Map<TopicPartition, Long> endOffsets = consumer.endOffsets(assignment);

        Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
        endOffsets.forEach((partition, offset) -> {
            offsetsToCommit.put(partition, new OffsetAndMetadata(offset));
        });

        consumer.commitSync(offsetsToCommit);
        logger.info("Commited offsets {}", offsetsToCommit);
        consumer.close();
    }



}
