package com.example.dataconsumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
public class KafkaConsumerApi {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafKaStreamApi.class);
    public static void main(String[] args) {
        MyKafkaConsumerFactory kafkaConsumerFactory = new MyKafkaConsumerFactory();
        KafkaConsumer<byte[], String> consumer = kafkaConsumerFactory.getConsumer();

        while (true) {
            ConsumerRecords<byte[], String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<byte[], String> record : records)
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
        }

        // Close the consumer when necessary
        // consumer.close();

    }

    private static class MyKafkaConsumerFactory {

        private final String clientId = "KafkaConsumerApi";
        private final String groupId = "mygroupId";
        private final String endpoints = "localhost:9092";
        private final String topic = "input-topic";
        private final String autoOffsetResetPolicy = "earliest";
        private final String securityProtocol = "SASL_SSL";
        private final String securitySaslMechanism = "SCRAM-SHA-256";
        private final String keyDeserializer = ByteArrayDeserializer.class.getCanonicalName();
        private final String valueDeserializer = StringDeserializer.class.getCanonicalName();

        public KafkaConsumer<byte[], String> getConsumer() {
            KafkaConsumer<byte[], String> consumer = new KafkaConsumer<>(getProperties());
            consumer.subscribe(Collections.singletonList(topic));
            return consumer;
        }

        private Properties getProperties() {
            Properties props = new Properties();
            props.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, endpoints);
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetResetPolicy);
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);
//            props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);
            props.put(SaslConfigs.SASL_MECHANISM, securitySaslMechanism);
            return props;
        }

    }

}

