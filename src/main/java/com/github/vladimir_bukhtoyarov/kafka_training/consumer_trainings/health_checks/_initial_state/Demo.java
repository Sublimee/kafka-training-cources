package com.github.vladimir_bukhtoyarov.kafka_training.consumer_trainings.health_checks._initial_state;

import com.github.vladimir_bukhtoyarov.kafka_training.consumer_trainings.util.Constants;
import com.github.vladimir_bukhtoyarov.kafka_training.consumer_trainings.util.InfiniteIterator;
import com.github.vladimir_bukhtoyarov.kafka_training.consumer_trainings.util.Message;
import com.github.vladimir_bukhtoyarov.kafka_training.consumer_trainings.util.Producer;
import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.Bucket;
import io.github.bucket4j.Bucket4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.util.*;

public class Demo {

    // ********* Plan **************
    // 1 - Start producer.
    //     Show few messages via admin panel.
    //     Explain producer logs(partitions, offset).
    //
    // 2 - Start consumer-1, show consumer logs.
    //     Point that consumer-1 listen all partitions.
    //     Point to the offset from that consumer started to listen.
    //
    // 3 - Restart  consumer-1,
    //     Point to the offset from that consumer started to listen.
    //     Explain why "auto.offset.reset" is very major option.
    //
    // 4 - Start consumer-2,
    //     Show consumer group on topic panel, point to the unfair partitioning.
    //
    // 5 - Start consumer-3, Show consumer group on topic panel, point to the balanced partitioning.

    // 6 - Start consumer-4, show consumer logs.
    //     Point that consumer-4 listen all partitions.
    //     Point to the offset from that consumer started to listen, describe the differences with consumer-1

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

    private static final class StartConsumer_1 {
        public static void main(String[] args) {
            Consumer consumer = new Consumer("consumer-1", Collections.singleton(Constants.TOPIC));
            consumer.start();
        }
    }

    private static final class StartConsumer_2 {
        public static void main(String[] args) {
            Consumer consumer = new Consumer("consumer-2", Collections.singleton(Constants.TOPIC));
            consumer.start();
        }
    }

    private static final class StartConsumer_3 {
        public static void main(String[] args) {
            Consumer consumer = new Consumer("consumer-3", Collections.singleton(Constants.TOPIC));
            consumer.start();
        }
    }

    private static final class StartConsumer_4 {
        public static void main(String[] args) {
            Map<String, Object> overrides = new HashMap<>();
            overrides.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            overrides.put(ConsumerConfig.GROUP_ID_CONFIG, "group-2");
            Consumer consumer = new Consumer("consumer-4", Collections.singleton(Constants.TOPIC), overrides);
            consumer.start();
        }
    }


}
