package org.jboss.pnc.logprocessor.eventduration;

import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.jboss.pnc.logprocessor.eventduration.domain.LogEvent;
import org.jboss.pnc.logprocessor.eventduration.utils.PropertiesUtil;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


/**
 * @author Ales Justin
 * @author <a href="mailto:matejonnet@gmail.com">Matej Lazar</a>
 */
public class Application {
    public static final String INPUT_TOPIC = "input-topic";
    public static final String LOG_STORE = "log-store";

    private KafkaStreams streams;

    public Application(String[] args) throws Exception {
        Properties properties = PropertiesUtil.properties(args);
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "registry-demo");

        StreamsBuilder builder = new StreamsBuilder();

        // Topology

        Map<String, String> configuration = new HashMap<>();
        configuration.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
        configuration.put(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG, "0");
        configuration.put(TopicConfig.SEGMENT_BYTES_CONFIG, String.valueOf(64 * 1024 * 1024));

        Serde<LogEvent> logSerde = Serdes.serdeFrom(
            new LogEvent.JsonSerializer(),
            new LogEvent.JsonDeserializer()
        );
        KStream<String, LogEvent> input = builder.stream(
            INPUT_TOPIC,
            Consumed.with(Serdes.String(), logSerde)
        );

        StoreBuilder<KeyValueStore<String, LogEvent>> storageStoreBuilder =
            Stores
                .keyValueStoreBuilder(
                    Stores.inMemoryKeyValueStore(LOG_STORE),
                    Serdes.String(), logSerde
                )
                .withCachingEnabled()
                .withLoggingEnabled(configuration);
        builder.addStateStore(storageStoreBuilder);

        KStream<String, LogEvent> output = input.transform(
            MergeTransformer::new,
            LOG_STORE
        );

        // for Kafka console consumer show-case, pure String
//TODO        output.mapValues(value -> String.format("Log diff: %s", Math.abs(value.getSnd() - value.getFst())))
//              .to("logx-topic", Produced.with(Serdes.String(), Serdes.String()));

        output.to("logx-topic", Produced.with(Serdes.String(), logSerde));

        output.filter((key, logEvent) -> isEndLogEvent(logEvent))
              .to("dbx-topic", Produced.with(Serdes.String(), logSerde));

        Topology topology = builder.build(properties);
        streams = new KafkaStreams(topology, properties);
    }

    private boolean isEndLogEvent(LogEvent logEvent) {
        return logEvent.getEventType().isPresent() && logEvent.getEventType().get().equals(LogEvent.EventType.END);
    }

    public void start() {
        streams.start();
    }

    public void stop() {
        if (streams != null) {
            streams.close();
        }
    }
}
