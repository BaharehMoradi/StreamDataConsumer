
## Kafka commands:


#### Send messages with key-value pairs to a Kafka topic using the Kafka console producer
- .\bin\windows\kafka-console-producer.bat --bootstrap-server localhost:9092 --topic input-topic --property "parse.key=true" --property "key.separator=:"

After running the command, you can start typing your messages in the console. Each message should be entered in the format key:value.
- key1:value1
- key2:value2