package com.example.dataconsumer;

import org.testcontainers.junit.jupiter.Testcontainers;

import static org.junit.Assert.assertEquals;

@Testcontainers
class KafKaStreamApiTest {
//    @Container
//    public static KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.0"));
//
//    private static KafkaProducer<String, String> producer;
//
//    @BeforeClass
//    public static void setup() {
//        String bootstrapServers = kafkaContainer.getBootstrapServers();
//
//        // Create producer properties
//        Properties producerProps = new Properties();
//        producerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
//        producerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
//        producerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
//
//        // Create and configure the Kafka producer
//        producer = new KafkaProducer<>(producerProps, new StringSerializer(), new StringSerializer());
//    }
//
//    @AfterClass
//    public static void cleanup() {
//        producer.close();
//    }
//
//    @Test
//    public void testMessageProcessing() {
//        String testTopic = "input-topic";
//        String testKey = "key";
//        String testValue = "value";
//
//        // Create consumer properties
//        Properties consumerProps = new Properties();
//        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
//        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
//        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
//        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
//        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//
//        // Create and configure the Kafka consumer
//        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
//        consumer.subscribe(Collections.singleton(testTopic));
//
//        // Produce a test message
//        producer.send(new ProducerRecord<>(testTopic, testKey, testValue));
//        producer.flush();
//
//        // Poll for records
//        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
//        assertEquals(1, records.count());
//
//        // Verify the consumed message
//        ConsumerRecord<String, String> record = records.iterator().next();
//        assertEquals(testKey, record.key());
//        assertEquals(testValue, record.value());
//
//        consumer.close();
//    }
}