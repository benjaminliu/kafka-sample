package com.ben.kafkasample.config;

import com.ben.kafkasample.handler.MyKafkaHandler;
import com.ben.kafkasample.kafka.MyKafkaConsumerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;

/**
 * 客票验真kafka消费者配置 <br>
 *
 * @author: 刘恒 <br>
 * @date: 2018/11/19 <br>
 */
@Configuration
public class MyConsumerConfig {
    @Value("${mykafka.consumer.ticketvalid.servers}")
    private String servers;

    @Value("${mykafka.consumer.ticketvalid.topic}")
    private String topic;

    @Value("${mykafka.consumer.ticketvalid.groupid}")
    private String groupId;

    @Autowired
    private MyKafkaHandler ticketValidKafkaHandler;

    @Bean("myListener")
    public KafkaMessageListenerContainer<String, String> ticketValidKafkaMessageListenerContainer() {
        KafkaMessageListenerContainer<String, String> kafkaListenerContainer = MyKafkaConsumerFactory.createEarliestKafkaListenerContainer(
                servers,
                topic,
                groupId,
                false,
                ticketValidKafkaHandler);

        return kafkaListenerContainer;
    }
}
