package com.abhirockzz;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueMapper;

public class KafkaStreamsApp {

    final static String INPUT_TOPIC = "lower-case";
    final static String OUTPUT_TOPIC = "upper-case";
    final static String KAFKA_BROKER_ENV_VAR = "KAFKA_BROKER";
    final static String APP_ID = "upper-to-lower-app";

    public static void main(String[] args) throws InterruptedException {

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> lowerCaseStrings = builder.stream(INPUT_TOPIC);
        KStream<String, String> upperCaseStrings = lowerCaseStrings.mapValues(new ValueMapper<String, String>() {
            @Override
            public String apply(String str) {
                System.out.println("converting " + str + " to upper case");
                return str.toUpperCase();
            }
        });
        upperCaseStrings.to(OUTPUT_TOPIC);

        Topology topology = builder.build();
        KafkaStreams streamsApp = new KafkaStreams(topology, getKafkaStreamsConfig());
        streamsApp.start();
        System.out.println("Started streams app.....");

        Runtime.getRuntime().addShutdownHook(new Thread(streamsApp::close));
        new CountDownLatch(1).await();
    }

    static Properties getKafkaStreamsConfig() {

        String kafkaBroker = System.getenv().get(KAFKA_BROKER_ENV_VAR);
        System.out.println("Kafka Broker " + kafkaBroker);

        Properties configurations = new Properties();

        configurations.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker + ":9092");
        configurations.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID);
        configurations.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        configurations.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        configurations.put(StreamsConfig.REQUEST_TIMEOUT_MS_CONFIG, "20000");
        configurations.put(StreamsConfig.RETRY_BACKOFF_MS_CONFIG, "500");

        return configurations;
    }
}
