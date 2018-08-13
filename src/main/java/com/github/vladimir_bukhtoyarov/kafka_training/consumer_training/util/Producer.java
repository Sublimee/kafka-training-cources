package com.github.vladimir_bukhtoyarov.kafka_training.consumer_training.util;


import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.Bucket;
import io.github.bucket4j.Bucket4j;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Iterator;
import java.util.Properties;
import java.util.UUID;


public class Producer {

    private static final Logger logger = LoggerFactory.getLogger(Producer.class);

    private final KafkaProducer<String, Message> producer;

    public Producer() {
        final Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.bootstrapServers);
        this.producer = new KafkaProducer<>(properties, new StringSerializer(), new JsonSerDer());
    }

    public void send(ProducerRecord<String, Message> record) {
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception != null) {
                    logger.error("failed to send {}", record.value(), exception);
                } else {
                    logger.info("sent message {} to partition {} with offset {}", record.value(), metadata.partition(), metadata.offset());
                }
            }
        });
    }

    public void send(Bucket rateLimiter, Iterator<ProducerRecord<String, Message>> records) {
        while (records.hasNext()) {
            rateLimiter.asScheduler().consumeUninterruptibly(1);
            ProducerRecord<String, Message> record = records.next();
            send(record);
        }
    }

    private static final class SelfTest {

    }

}
