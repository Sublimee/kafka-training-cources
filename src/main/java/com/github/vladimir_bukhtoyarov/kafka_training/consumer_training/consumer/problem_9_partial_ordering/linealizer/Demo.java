package com.github.vladimir_bukhtoyarov.kafka_training.consumer_training.consumer.problem_9_partial_ordering.linealizer;

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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


public class Demo {

    // ********* Plan **************
    // 1 - Start consumer-1
    //
    // 2 - Start producer.
    //
    // 3 - Analyze consumer logs
    //      Point that message processing is ordered inside key
    //
    // 4 - Start InfiniteProducer. Wait to a significant lag.
    //     Point to the size of consumer queue, explain the small value of queue size.
    //
    // 5 - Wait to significant lag
    //     Start consumer 2-3 explain the lags of rebalancing

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
