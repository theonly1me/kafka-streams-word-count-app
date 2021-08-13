package com.github.theonly1me.kafka.streams;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic word-count-output --from-beginning --formatter kafka.tools.DefaultMessageFormatter --property print.key=true --property print.value=true --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
 */



@Slf4j
public class ConsumerApplication {
    public static void main(String[] args) {
        Properties properties = new Properties();
        String bootstrapServers="127.0.0.1:9092";
        String groupId = "my-app";
        //Create consumer config
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        // subscribe consumer
        //consumer.subscribe((Collections.singleton("first_topic"))); // to subscribe to 1 topic
        consumer.subscribe((Arrays.asList("word-count-output"))); //to subscribe to multiple topics

        //poll for new data
        while(true){
            ConsumerRecords<String, String> records =  consumer.poll(Duration.ofMillis(100)); //new in Kafka 2.0.0
            for(ConsumerRecord record : records){
                log.info("Key" + record.key() + "Value" + record.value());
                log.info("Partition" + record.partition() + "Value" + record.offset());
            }
        }
    }
}

