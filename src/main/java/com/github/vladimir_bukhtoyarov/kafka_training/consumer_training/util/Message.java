package com.github.vladimir_bukhtoyarov.kafka_training.consumer_training.util;

public class Message {

    private String payload;

    private Integer delayMillis;

    private String errorClass;

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    public Integer getDelayMillis() {
        return delayMillis;
    }

    public void setDelayMillis(Integer delayMillis) {
        this.delayMillis = delayMillis;
    }

    public String getErrorClass() {
        return errorClass;
    }

    public void setErrorClass(String errorClass) {
        this.errorClass = errorClass;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Message{");
        sb.append("payload='").append(payload).append('\'');
        if (delayMillis != null) {
            sb.append(", delayMillis=").append(delayMillis);
        }
        if (errorClass != null) {
            sb.append(", errorClass='").append(errorClass).append('\'');
        }
        sb.append('}');
        return sb.toString();
    }

}
