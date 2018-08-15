package com.github.vladimir_bukhtoyarov.kafka_training.consumer_trainings.health_checks.problem_2_missed_topics;

import com.github.vladimir_bukhtoyarov.kafka_training.consumer_trainings.util.Constants;
import com.github.vladimir_bukhtoyarov.kafka_training.consumer_trainings.util.InfiniteIterator;
import com.github.vladimir_bukhtoyarov.kafka_training.consumer_trainings.util.Message;
import com.github.vladimir_bukhtoyarov.kafka_training.consumer_trainings.util.Producer;
import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.Bucket;
import io.github.bucket4j.Bucket4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

public class Demo {

    // ********* Plan **************
    // 1 - Start producer, Start consumer
    //
    // 2 - Show consumer logs
    //     Point that missed topic detected by health-check
    //
    // 3 - Run CreateTopic
    //     Show consumer log, point that health-check is healthy
    //     Describe the differences in missed topic handling in 1.0 and 0.10


    private static final class StartConsumer_1 {
        public static void main(String[] args) {
            Set<String> topics = new HashSet<>(Arrays.asList(
                    Constants.TOPIC, "not-existed"
            ));
            Consumer consumer = new Consumer("consumer-1", topics);
            consumer.start();
            initHealthCheck(consumer);
        }
    }

    private static final class StartProducer {
        public static void main(String[] args) {
            Bandwidth bandwidth = Bandwidth.simple(1, Duration.ofSeconds(3))
                    .withInitialTokens(0);
            Bucket rateLimiter = Bucket4j.builder().addLimit(bandwidth).build();

            Iterator<ProducerRecord<String, Message>> records = new InfiniteIterator<>(() -> {
                Message message = new Message();
                message.setPayload(UUID.randomUUID().toString());
                return new ProducerRecord<>(Constants.TOPIC, message);
            });

            Producer producer = new Producer();
            producer.send(rateLimiter, records);
        }
    }

    private static void initHealthCheck(Consumer consumer) {
        Logger logger = LoggerFactory.getLogger("health-check");
        Timer healthCheckTimer = new Timer("Consumer health-checker");
        healthCheckTimer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                try {
                    Consumer.HealthStatus status = consumer.getHealth();
                    if (status.isHealthy()) {
                        logger.info("" + status);
                    } else {
                        logger.error("" + status);
                    }
                } catch (Throwable t) {
                    logger.error("Failed to check health of consumer", t);
                }
            }
        }, 10000, 10000);
    }

}
