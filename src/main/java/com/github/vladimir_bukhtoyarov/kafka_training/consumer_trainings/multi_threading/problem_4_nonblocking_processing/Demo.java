package com.github.vladimir_bukhtoyarov.kafka_training.consumer_trainings.multi_threading.problem_4_nonblocking_processing;

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
    // 1 - Start consumer, point that consumer is single-threaded
    //
    // 2 - Start producer.
    //
    // 3 - Point that lag is small, consumer is not overloaded

    private static final class InfiniteProducer {
        public static void main(String[] args) {
            Iterator<ProducerRecord<String, Message>> records = new InfiniteIterator<>(() -> {
                String random = UUID.randomUUID().toString();
                Message message = new Message();
                message.setPayload(random);
                message.setDelayMillis(1000);
                return new ProducerRecord<>(Constants.TOPIC, random, message);
            });

            Producer producer = new Producer();
            producer.send(400, Duration.ofSeconds(1), records);
        }
    }

    private static final class StartConsumer_1 {
        public static void main(String[] args) {
            Set<String> topics = new HashSet<>(Collections.singleton(Constants.TOPIC));
            BlockingQueue queue = new LinkedBlockingQueue();
            ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, Long.MAX_VALUE, TimeUnit.MILLISECONDS, queue);

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
