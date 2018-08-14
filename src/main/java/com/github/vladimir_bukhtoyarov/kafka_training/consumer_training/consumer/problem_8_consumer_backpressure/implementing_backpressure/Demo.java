package com.github.vladimir_bukhtoyarov.kafka_training.consumer_training.consumer.problem_8_consumer_backpressure.implementing_backpressure;

import com.github.vladimir_bukhtoyarov.kafka_training.consumer_training.util.Constants;
import com.github.vladimir_bukhtoyarov.kafka_training.consumer_training.util.InfiniteIterator;
import com.github.vladimir_bukhtoyarov.kafka_training.consumer_training.util.Message;
import com.github.vladimir_bukhtoyarov.kafka_training.consumer_training.util.Producer;
import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.Bucket;
import io.github.bucket4j.Bucket4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class Demo {

    // ********* Plan **************
    // 1 - Start consumer-1
    //    Wait until lag become to zero
    //
    // 2 - Start producer.
    //     Point that queue of consumer approximately equals to backpressure value
    //     Point to overloading health-check

    private static final class StartProducer {
        public static void main(String[] args) {
            Bandwidth bandwidth = Bandwidth.simple(500, Duration.ofSeconds(1))
                    .withInitialTokens(0);
            Bucket rateLimiter = Bucket4j.builder().addLimit(bandwidth).build();

            Iterator<ProducerRecord<String, Message>> records = new InfiniteIterator<>(() -> {
                Message message = new Message();
                message.setPayload(UUID.randomUUID().toString());
                message.setDelayMillis(1000);
                return new ProducerRecord<>(Constants.TOPIC, message);
            });

            Producer producer = new Producer();
            producer.send(rateLimiter, records);
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
