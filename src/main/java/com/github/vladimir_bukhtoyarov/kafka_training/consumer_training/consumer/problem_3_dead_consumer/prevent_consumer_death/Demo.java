package com.github.vladimir_bukhtoyarov.kafka_training.consumer_training.consumer.problem_3_dead_consumer.prevent_consumer_death;

import com.github.vladimir_bukhtoyarov.kafka_training.consumer_training.util.Constants;
import com.github.vladimir_bukhtoyarov.kafka_training.consumer_training.util.Message;
import com.github.vladimir_bukhtoyarov.kafka_training.consumer_training.util.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public class Demo {

    // ********* Plan **************
    // 1 - Start consumer-1, start producer.
    //     Check that consumer survives
    //
    // 2 - Stop kafka server.
    //     Check that consumer survives.


    private static final class StartConsumer_1 {
        public static void main(String[] args) {
            Set<String> topics = new HashSet<>(Collections.singleton(Constants.TOPIC));
            Consumer consumer = new Consumer("consumer-1", topics);
            consumer.start();
            initHealthCheck(consumer);
        }
    }

    private static final class StartProducer {
        public static void main(String[] args) {
            Producer producer = new Producer();
            while (true) {
                List<ProducerRecord<String, Message>> records = new ArrayList<>();
                for (int i = 0; i < 10; i++) {
                    Message message = new Message();
                    message.setPayload("" + i);
                    if (i == 9) {
                        message.setErrorClass(IOException.class.getName());
                    }
                    records.add(new ProducerRecord<>(Constants.TOPIC, message));
                }
                producer.send(records);
                LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(5));
            }
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
