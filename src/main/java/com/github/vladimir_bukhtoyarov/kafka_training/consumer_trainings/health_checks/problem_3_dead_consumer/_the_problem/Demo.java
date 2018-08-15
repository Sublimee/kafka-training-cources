package com.github.vladimir_bukhtoyarov.kafka_training.consumer_trainings.health_checks.problem_3_dead_consumer._the_problem;

import com.github.vladimir_bukhtoyarov.kafka_training.consumer_trainings.util.Constants;
import com.github.vladimir_bukhtoyarov.kafka_training.consumer_trainings.util.Message;
import com.github.vladimir_bukhtoyarov.kafka_training.consumer_trainings.util.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class Demo {

    // ********* Plan **************
    // 1 - Start consumer
    //
    // 2 - Start producer. Wait for consumer death.
    //     Point that health-check is wrong.
    //
    // 3 -  Mention that closing of consumer does not lead to unsubscribe


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
            List<ProducerRecord<String, Message>> records = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                Message message = new Message();
                message.setPayload("" + i);
                if (i == 9) {
                    message.setErrorClass(IOException.class.getName());
                }
                records.add(new ProducerRecord<>(Constants.TOPIC, message));
            }
            Producer producer = new Producer();
            producer.send(records);
            producer.stop();
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
