package com.github.vladimir_bukhtoyarov.kafka_training.consumer_trainings.health_checks.problem_1_consumer_starvation._the_problem;

import com.github.vladimir_bukhtoyarov.kafka_training.consumer_trainings.util.Constants;
import com.github.vladimir_bukhtoyarov.kafka_training.consumer_trainings.util.InfiniteIterator;
import com.github.vladimir_bukhtoyarov.kafka_training.consumer_trainings.util.Message;
import com.github.vladimir_bukhtoyarov.kafka_training.consumer_trainings.util.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.UUID;

public class Demo {

    // ********* Plan **************
    // 1 - Start producer and consumers 1,2,3,4
    //
    // 2 - Show that one consumer reads nothing and there are no errors in the logs.
    //     Describe why health-check to cover this problem has matter.


    private static final class StartProducer {
        public static void main(String[] args) {
            Iterator<ProducerRecord<String, Message>> records = new InfiniteIterator<>(() -> {
                Message message = new Message();
                message.setPayload(UUID.randomUUID().toString());
                return new ProducerRecord<>(Constants.TOPIC, message);
            });

            Producer producer = new Producer();
            producer.send(1, Duration.ofSeconds(3), records);
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
            Consumer consumer = new Consumer("consumer-4", Collections.singleton(Constants.TOPIC));
            consumer.start();
        }
    }

}
