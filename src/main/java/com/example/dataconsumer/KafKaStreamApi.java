package com.example.dataconsumer;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.StreamsConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

//Consume Data through the Kafka Stream API
public class KafKaStreamApi {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafKaStreamApi.class);

    public static void main(String[] args) {
        MyKafkaStreamsFactory myKafkaStreamsFactory = new MyKafkaStreamsFactory();
        KafkaStreams kafkaStreams = myKafkaStreamsFactory.getKafkaStreams();
        kafkaStreams.setUncaughtExceptionHandler(myKafkaStreamsFactory.getUncaughtExceptionHandler());
        kafkaStreams.start();
        LOGGER.info("kafka stream API started");
        // Close the Stream when necessary
        // kafkaStreams.close()
    }
    private static class MyKafkaStreamsFactory {
        private String clientId = "KafKaStreamApi";
        private String groupId = "mygroupId";
        private String endpoints = "localhost:9092";
        private String topic = "input-topic";
        private String autoOffsetResetPolicy = "earliest";
        private String streamsNumOfThreads = "3";
        private String securityProtocol = "SASL_SSL";
        private String securitySaslMechanism = "SCRAM-SHA-256";
        private String keySerde = "org.apache.kafka.common.serialization.Serdes$LongSerde";
        private String valueSerde = "org.apache.kafka.common.serialization.Serdes$StringSerde";
        private String deserializationExceptionHandler = LogAndContinueExceptionHandler.class.getCanonicalName();

        private KafkaStreams getKafkaStreams() {

            StreamsBuilder streamsBuilder = new StreamsBuilder();
            KStream<String, String> stream = streamsBuilder.stream(topic, Consumed.with(Serdes.String(), Serdes.String()));
            stream.foreach((key, value) -> System.out.printf("key= %s , value=%s%n", key, value));

            return new KafkaStreams(streamsBuilder.build(), getProperties());
        }

        private Properties getProperties() {
            Properties props = new Properties();
            props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetResetPolicy);
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keySerde);
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueSerde);
            props.put(StreamsConfig.APPLICATION_ID_CONFIG, clientId);
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, endpoints);
            props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, streamsNumOfThreads);
//            props.put(StreamsConfig.SECURITY_PROTOCOL_CONFIG, securityProtocol);
            props.put(SaslConfigs.SASL_MECHANISM, securitySaslMechanism);
            props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, deserializationExceptionHandler);

            return props;
        }

        private Thread.UncaughtExceptionHandler getUncaughtExceptionHandler() {
            return (thread, exception) -> System.out.println("Exception running the Stream " + exception.getMessage());
        }
    }
}

