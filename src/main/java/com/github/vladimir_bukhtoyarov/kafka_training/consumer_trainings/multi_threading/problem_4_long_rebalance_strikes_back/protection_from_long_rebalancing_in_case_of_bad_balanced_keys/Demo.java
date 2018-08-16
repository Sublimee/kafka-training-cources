package com.github.vladimir_bukhtoyarov.kafka_training.consumer_trainings.multi_threading.problem_4_long_rebalance_strikes_back.protection_from_long_rebalancing_in_case_of_bad_balanced_keys;

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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;


public class Demo {

    // ********* Plan **************
    // 1 - Start consumer-1
    //
    // 2 - Start producer.
    //
    //
    // 3 - Start InfiniteProducer.
    //
    // 4 - Wait to significant lag
    //     Start consumer 2-3
    //     Check that rebalancing was fast

    private static final class StartInfiniteProducer {
        public static void main(String[] args) {
            Bandwidth bandwidth = Bandwidth.simple(400, Duration.ofSeconds(1))
                    .withInitialTokens(0);
            Bucket rateLimiter = Bucket4j.builder().addLimit(bandwidth).build();

            AtomicInteger sequence = new AtomicInteger();
            Iterator<ProducerRecord<String, Message>> records = new InfiniteIterator<>(() -> {
                int i = sequence.getAndIncrement();
                String key = "" + i % 10;
                Message message = new Message();
                message.setPayload("" + i);
                message.setDelayMillis((i % 3) * 1000);
                return new ProducerRecord<>(Constants.TOPIC, key, message);
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

    private static final class StartConsumer_2 {
        public static void main(String[] args) {
            Set<String> topics = new HashSet<>(Collections.singleton(Constants.TOPIC));
            BlockingQueue queue = new LinkedBlockingQueue();
            ThreadPoolExecutor executor = new ThreadPoolExecutor(100, 100, Long.MAX_VALUE, TimeUnit.MILLISECONDS, queue);

            Consumer consumer = new Consumer("consumer-1", topics, executor);
            consumer.start();
            initHealthCheck(consumer, executor);
        }
    }

    private static final class StartConsumer_3 {
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
        Linealizer linealizer = consumer.getLinealizer();
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
                logger.info("Executor queue size is {}, active thread count is {}, linelizer queues size {}",
                        executor.getQueue().size(),
                        executor.getActiveCount(),
                        linealizer.getSize());
            }
        }, 10000, 10000);
    }

}
