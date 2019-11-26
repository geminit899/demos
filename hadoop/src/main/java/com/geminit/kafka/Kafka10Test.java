package com.geminit.kafka;


/**
 * Created by root on 7/26/19.
 */
public class Kafka10Test {
    public static void main(String[] args) {
//        Properties properties = new Properties();
//        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
//        properties.put("group.id", "test");
//        Consumer<byte[], byte[]> consumer =
//                new KafkaConsumer<>(properties, new ByteArrayDeserializer(), new ByteArrayDeserializer());
//
//        List<PartitionInfo> partitionInfos = consumer.partitionsFor("SparkBatchInputTest");
//
//        List<TopicPartition> topicPartitions = partitionInfos.stream()
//                .map(info -> new TopicPartition(info.topic(), info.partition()))
//                .collect(Collectors.toList());
//
//        Map<TopicPartition, Long> offsets = new HashMap<>();
//        for (TopicPartition topicAndPartition : topicPartitions) {
//            long offset = consumer.position(topicAndPartition);
//            offsets.put(topicAndPartition, offset);
//        }
//
//        System.out.print("yes");
    }
}
