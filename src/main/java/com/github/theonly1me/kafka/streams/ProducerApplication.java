package com.github.theonly1me.kafka.streams;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Slf4j
public class ProducerApplication {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        String bootstrapServers = "127.0.0.1:9092";
        //Create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // Create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for(int i=0; i<1000; i++){
            //create a producer record
            String topic = "word-count-input";
            String value = i%2==0 ? "kafka str test" : "streams are cool";
            value.concat(" " + Integer.toString(i));
            String key = "id_" + Integer.toString(i);

            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
            log.info("Key is {}", key);
            //send data
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    //executes everytime a record is sent or an exception is thrown
                    if(e == null){
                        log.info("Received new metadata. \n" +
                                "Topic: " + metadata.topic() + "\n" +
                                "Partitions: " + metadata.partition() + "\n" +
                                "Offsets: " + metadata.offset() + "\n" +
                                "Timestamps: " + metadata.timestamp());
                    } else {
                        log.error("Error while producing", e);
                    }
                }
            }).get(); //blocking .send() to make it synchronous for testing purpose. Not to be done in production
        }

        producer.flush();
        producer.close(); //flush and close
    }
}
