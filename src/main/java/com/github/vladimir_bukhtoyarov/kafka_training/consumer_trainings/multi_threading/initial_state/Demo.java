package com.github.vladimir_bukhtoyarov.kafka_training.consumer_trainings.multi_threading.initial_state;

import com.github.vladimir_bukhtoyarov.kafka_training.consumer_trainings.util.Constants;
import com.github.vladimir_bukhtoyarov.kafka_training.consumer_trainings.util.InfiniteIterator;
import com.github.vladimir_bukhtoyarov.kafka_training.consumer_trainings.util.Message;
import com.github.vladimir_bukhtoyarov.kafka_training.consumer_trainings.util.Producer;
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
    // 0 - Describe the changes in consumer related to multi-threading consumption:
    //        - Ordered commits inside partition
    //        - Complexity of rebalance listener
    //        - Graceful shutdown
    //
    // 1 - Start consumer
    //
    // 2. - Start producer.
    //     Wait for a 30 seconds.
    //
    // 3. - Show that there are no lag.
    //
    // 4. - Stop consumer.
    //     Wait 30 seconds.
    //     Show the lag.
    //
    // 5. - Start consumer again, show that lag decreasing.
    //
    //
    // Cleanup:
    // * Stop producer and all consumers.
    // * Reset offset via SeekToEnd

    private static final class StartProducer {
        public static void main(String[] args) {
            Iterator<ProducerRecord<String, Message>> records = new InfiniteIterator<>(() -> {
                Message message = new Message();
                message.setPayload(UUID.randomUUID().toString());
                message.setDelayMillis(1000);
                return new ProducerRecord<>(Constants.TOPIC, message);
            });

            Producer producer = new Producer();
            producer.send(100, Duration.ofSeconds(1), records);
        }
    }

    public static void main(String[] args) {
        System.out.println(true & true);
    }

    private static final class StartConsumer_1 {
        public static void main(String[] args) {
            Set<String> topics = new HashSet<>(Collections.singleton(Constants.TOPIC));
            BlockingQueue queue = new LinkedBlockingQueue();
            ThreadPoolExecutor executor = new ThreadPoolExecutor(200, 200, Long.MAX_VALUE, TimeUnit.MILLISECONDS, queue);

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
