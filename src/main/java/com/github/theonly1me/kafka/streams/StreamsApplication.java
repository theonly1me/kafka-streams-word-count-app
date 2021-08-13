package com.github.theonly1me.kafka.streams;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;

import java.util.Arrays;
import java.util.Properties;

@Slf4j
public class StreamsApplication {
    public static void main(String[] args) {
        //configuring Streams properties
        String bootstrapServer = "127.0.0.1:9092";
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "count-words");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        //APP LOGIC
        //Step 0. Create a StreamBuilder
        StreamsBuilder builder = new StreamsBuilder();

        //Step 1. Stream from Kafka
        KStream<String, String> wordCountInput =  builder.stream("word-count-input");

        //Step 2. MapValues (lowercase)
        //Step 3. FlatMap Values splitting by space
        //Step 4. SelectKey to apply a key
        //Step 5. GroupByKey before aggregation
        //Step 6. Count/Aggregate occurrences in each group
        KTable<String, Long> wordCounts = wordCountInput.mapValues(value -> value.toLowerCase())
                .flatMapValues(values -> Arrays.asList(values.split(" ")))
                .selectKey((key, value) -> value)
                .groupByKey()
                .count(Named.as("WordCounts"));

        //Step 7. Write results back TO Kafka
        wordCounts.toStream().to("word-count-output");

        KafkaStreams streams = new KafkaStreams(builder.build(), properties);
        streams.start();

        //logging the topology
        log.info(streams.toString());

        //Add shutdown hook
        //alternatively we can add a timeout
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
