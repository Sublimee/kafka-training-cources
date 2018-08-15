package com.github.vladimir_bukhtoyarov.kafka_training.consumer_training.consumer.problem_9_partial_ordering._the_problem;

import com.github.vladimir_bukhtoyarov.kafka_training.consumer_training.util.Constants;
import com.github.vladimir_bukhtoyarov.kafka_training.consumer_training.util.Message;
import com.github.vladimir_bukhtoyarov.kafka_training.consumer_training.util.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


public class Demo {

    // ********* Plan **************
    // 1 - Start consumer-1
    //
    // 2 - Start producer.
    //
    // 3 - Analyze consumer logs
    //      Point that:
    //          "2" reordered with "12"
    //          "5" reordered with "15"

    private static final class StartProducer {
        public static void main(String[] args) {
            Producer producer = new Producer();
            for (int i = 0; i < 100; i++) {
                String key = "" + i % 10;
                Message message = new Message();
                message.setPayload("" + i);
                message.setDelayMillis((i % 3) * 1000);
                ProducerRecord<String, Message> record = new ProducerRecord<>(Constants.TOPIC, key, message);
                producer.send(record);
            }
            producer.stop();
        }
    }

    private static final class StartConsumer_1 {
        public static void main(String[] args) {
            Set<String> topics = new HashSet<>(Collections.singleton(Constants.TOPIC));
            BlockingQueue queue = new LinkedBlockingQueue();
            ThreadPoolExecutor executor = new ThreadPoolExecutor(100, 100, Long.MAX_VALUE, TimeUnit.MILLISECONDS, queue);

            Consumer consumer = new Consumer("consumer-1", topics, executor);
            consumer.start();
            initHealthCheck(consumer, executor);
        }
    }

    private static void initHealthCheck(Consumer consumer, ThreadPoolExecutor executor) {
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
                logger.info("Executor queue size is {}, active thread count is {}", executor.getQueue().size(), executor.getActiveCount());
            }
        }, 10000, 10000);
    }

}
