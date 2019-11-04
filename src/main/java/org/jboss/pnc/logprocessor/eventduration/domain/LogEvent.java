package org.jboss.pnc.logprocessor.eventduration.domain;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;

/**
 * @author <a href="mailto:matejonnet@gmail.com">Matej Lazar</a>
 */
public class LogEvent {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private Instant time;

    public String getIdentifier() {
        return null; //TODO
    }

    public Instant getTime() {
        return time;
    }

    public enum EventType {
        START, END;
    }

    private Map<String, Object> message;

    public LogEvent(byte[] serialized) {
        JsonNode jsonNode = null;
        try {
            jsonNode = objectMapper.readTree(serialized);
        } catch (IOException e) {
            //TODO
            e.printStackTrace();
        }
        init(jsonNode);
    }

    public LogEvent(String serializedLogEvent) {
        JsonNode jsonNode = null;
        try {
            jsonNode = objectMapper.readTree(serializedLogEvent);
        } catch (IOException e) {
            //TODO
            e.printStackTrace();
        }
        init(jsonNode);
    }

    private void init(JsonNode jsonNode) {
        message = objectMapper.convertValue(jsonNode, new TypeReference<Map<String, Object>>() {

        });
        String time = (String) message.get("@timestamp");
        this.time = Instant.ofEpochMilli(Long.parseLong(time));
    }

    public void addDuration(Duration duration) {
        String logMessage = (String) message.get("message");
        message.put("message", logMessage + " Took " + duration + "ms.");
        message.put("operationTook", duration.toMillis());
    }

    public Optional<EventType> getEventType() {
        String eventType = (String) message.get("mdc.eventType");
        if (eventType != null) {
            return Optional.of(EventType.valueOf(eventType));
        } else {
            return Optional.empty();
        }
    }

    public static class JsonSerializer implements Serializer<LogEvent> {

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
        }

        @Override
        public byte[] serialize(String topic, LogEvent logEvent) {
            if (logEvent == null) {
                return null;
            }

            try {
                return objectMapper.writeValueAsBytes(logEvent.message);
            } catch (Exception e) {
                throw new SerializationException("Error serializing JSON message", e);
            }
        }

        @Override
        public void close() {
        }
    }

    public static class JsonDeserializer implements Deserializer<LogEvent> {

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
        }

        @Override
        public LogEvent deserialize(String topic, byte[] bytes) {
            if (bytes == null) {
                return null;
            }

            LogEvent data;
            try {
//                data = objectMapper.readValue(bytes, LogEvent.class);
                data = new LogEvent(bytes);
            } catch (Exception e) {
                throw new SerializationException(e);
            }
            return data;
        }

        @Override
        public void close() {
        }
    }
}
