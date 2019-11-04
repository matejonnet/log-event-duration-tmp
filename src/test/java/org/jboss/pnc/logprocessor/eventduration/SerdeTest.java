package org.jboss.pnc.logprocessor.eventduration;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.jboss.pnc.logprocessor.eventduration.domain.LogEvent;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * @author <a href="mailto:matejonnet@gmail.com">Matej Lazar</a>
 */
public class SerdeTest {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static final Logger logger = LoggerFactory.getLogger(SerdeTest.class);

    @Test
    public void shouldSerializeAndDeserializeLogEvent() throws JsonProcessingException {
        Map<String, String> map = new HashMap<>();
        map.put("key1", "val1");
        long now = System.currentTimeMillis();
        map.put("@timestamp", Long.toString(now));
        map.put("mdc.eventType", LogEvent.EventType.START.toString());

        String serializedInput = objectMapper.writeValueAsString(map);
        logger.info("Serialized input {}", serializedInput);

        LogEvent logEvent = new LogEvent(serializedInput);
        logEvent.getEventType().get().equals(LogEvent.EventType.START);

        byte[] serialized = new LogEvent.JsonSerializer().serialize("", logEvent);

        logger.info("Serialized {}", new String(serialized));

        new LogEvent.JsonDeserializer().deserialize("", serialized);

        logEvent.getEventType().get().equals(LogEvent.EventType.START);
        logEvent.getTime().equals(Long.toString(now));
    }
}
