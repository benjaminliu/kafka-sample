package com.ben.kafkasample.config;

import com.ben.kafkasample.kafka.DefaultProducerListerner;
import com.ben.kafkasample.kafka.MyKafkaProducerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

/**
 * 推送到嵩山kafka的配置 <br>
 *
 * @author: 刘恒 <br>
 * @date: 2018/11/19 <br>
 */
@Configuration
public class MyProducerConfig {

    @Value("${mykafka.producer.songsan.servers}")
    private String servers;

    @Value("${mykafka.producer.songsan.topic}")
    private String topic;

    @Bean("myProducer")
    public KafkaTemplate<String, String> songsanKafkaTemplate() {
        ProducerFactory<String, String> producer = MyKafkaProducerFactory.createProducer(servers);
        KafkaTemplate<String, String> kafkaTemplate = new KafkaTemplate<>(producer, true);
        kafkaTemplate.setDefaultTopic(topic);
        kafkaTemplate.setProducerListener(new DefaultProducerListerner());
        return kafkaTemplate;
    }
}
