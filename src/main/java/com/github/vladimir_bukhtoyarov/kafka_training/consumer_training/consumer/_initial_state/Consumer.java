package com.github.vladimir_bukhtoyarov.kafka_training.consumer_training.consumer._initial_state;

import com.github.vladimir_bukhtoyarov.kafka_training.consumer_training.util.Constants;
import com.github.vladimir_bukhtoyarov.kafka_training.consumer_training.util.JsonSerDer;
import com.github.vladimir_bukhtoyarov.kafka_training.consumer_training.util.Message;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public class Consumer {

    private static final Logger logger = LoggerFactory.getLogger(Consumer.class);

    private final KafkaConsumer<String, Message> consumer;
    private final Thread thread = new Thread(this::consumeInfinitely, "Kafka-consumer-event-loop");

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
        consumer.subscribe(topics);
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
            logger.error("Consumer is going to stop", t);
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

}
