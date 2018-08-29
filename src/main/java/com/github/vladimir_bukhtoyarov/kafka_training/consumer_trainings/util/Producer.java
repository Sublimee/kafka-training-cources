package com.github.vladimir_bukhtoyarov.kafka_training.consumer_trainings.util;


import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.Bucket;
import io.github.bucket4j.Bucket4j;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.Iterator;
import java.util.Properties;


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

    public void send(Collection<ProducerRecord<String, Message>> records) {
        for (ProducerRecord<String, Message> record : records) {
            send(record);
        }
    }

    public void send(int tokens, Duration period, Iterator<ProducerRecord<String, Message>> records) {
        Bucket rateLimiter = Bucket4j.builder()
                .addLimit(Bandwidth.simple(tokens, period).withInitialTokens(0))
                .build();

        while (records.hasNext()) {
            rateLimiter.asScheduler().consumeUninterruptibly(1);
            ProducerRecord<String, Message> record = records.next();
            send(record);
        }
    }

    public void stop() {
        producer.flush();
        producer.close();
    }

}
