package org.jboss.pnc.logprocessor.eventduration.utils;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

/**
 * @author Ales Justin
 */
public class PropertiesUtil {
    public static Properties properties(String[] args) {
        Properties properties = new Properties(System.getProperties());
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); //TODO
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "log-processor"); //TODO

        for (String arg : args) {
            if (arg.contains("=")) {
                String[] split = arg.split("=");
                properties.put(split[0], split[1]);
            }
        }
        return properties;
    }
}
