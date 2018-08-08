package com.github.vladimir_bukhtoyarov.kafka_training.consumer_training.util;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.Bucket;
import io.github.bucket4j.Bucket4j;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;


public class Producer {

    private static final Logger logger = LoggerFactory.getLogger(Producer.class);

    private final KafkaProducer<String, Message> producer;

    public Producer() {
        final Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.bootstrapServers);
        this.producer = new KafkaProducer<>(properties, new StringSerializer(), new JsonSerializer());
    }

    private static class JsonSerializer implements Serializer<Message> {

        private final ObjectMapper mapper = new ObjectMapper();

        public byte[] serialize(String topic, Message data) {
            try {
                return mapper.writeValueAsBytes(data);
            } catch (JsonProcessingException e) {
                throw new IllegalStateException(e);
            }
        }

        public void close() {
            // do nothing
        }

        public void configure(Map<String, ?> configs, boolean isKey) {
            // do nothing
        }
    }

    public void produce(Bucket rateLimiter, Iterator<ProducerRecord<String, Message>> records) {
        while (records.hasNext()) {
            rateLimiter.asScheduler().consumeUninterruptibly(1);
            ProducerRecord<String, Message> record = records.next();
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception != null) {
                        logger.error("Fail to send {}", record.value(), exception);
                    } else {
                        logger.info("sent message {} to partition {} with offset {}", record.value(), metadata.partition(), metadata.offset());
                    }
                }
            });
        }
    }

    private static final class SelfTest {
        public static void main(String[] args) {
            Bandwidth bandwidth = Bandwidth.simple(1, Duration.ofSeconds(5))
                    .withInitialTokens(0);
            Bucket rateLimiter = Bucket4j.builder().addLimit(bandwidth).build();

            Iterator<ProducerRecord<String, Message>> records = new InfiniteIterator<>(() -> {
                Message message = new Message();
                message.setPayload(UUID.randomUUID().toString());
                return new ProducerRecord<>(Constants.TOPIC, message);
            });

            Producer producer = new Producer();
            producer.produce(rateLimiter, records);
        }
    }

}
