package com.github.vladimir_bukhtoyarov.kafka_training.consumer_trainings.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.util.Map;

public class JsonSerDer implements Serializer<Message>, Deserializer<Message> {

    private final ObjectMapper mapper = new ObjectMapper();

    public byte[] serialize(String topic, Message data) {
        try {
            return mapper.writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public Message deserialize(String topic, byte[] data) {
        try {
            return mapper.readValue(data, Message.class);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public void close() {
        // do nothing
    }

    public void configure(Map<String, ?> configs, boolean isKey) {
        // do nothing
    }

}
