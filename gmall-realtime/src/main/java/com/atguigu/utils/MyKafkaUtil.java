package com.atguigu.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.lang.reflect.Field;
import java.util.Properties;

public class MyKafkaUtil {

    private static final String KAFKA_SERVER = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
    private static Properties properties = new Properties();
    private static final String DEFAULT_TOPIC = "dwd_default_topic";

    static {
        properties.setProperty("bootstrap.servers", KAFKA_SERVER);
    }

    public static FlinkKafkaProducer<String> getKafkaSink(String topic) {
//        properties.setProperty(ProducerConfig.ACKS_CONFIG, "0");
        return new FlinkKafkaProducer<String>(topic, new SimpleStringSchema(), properties);
    }

    public static <T> FlinkKafkaProducer<T> getKafkaSink(KafkaSerializationSchema<T> kafkaSerializationSchema) {
        return new FlinkKafkaProducer<T>(DEFAULT_TOPIC,
                kafkaSerializationSchema,
                properties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }

    public static FlinkKafkaConsumer<String> getKafkaSource(String topic, String groupId) {
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        return new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), properties);
    }

}
