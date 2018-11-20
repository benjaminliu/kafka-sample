package com.ben.kafkasample.kafka;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;

import java.util.HashMap;
import java.util.Map;

/**
 * KafkaMessageListener工厂类 <br>
 *
 * @author: 刘恒 <br>
 * @date: 2018/11/16 <br>
 */
public class MyKafkaConsumerFactory {
    private static Logger logger = LoggerFactory.getLogger(MyKafkaConsumerFactory.class);

    private static Map<String, Object> consumerConfigs(String servers, String groupId, String autoOffsetReset, boolean enableAutoCommit) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit);

        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "15000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return props;
    }

    public static KafkaMessageListenerContainer<String, String> createLatestKafkaListenerContainer(
            String servers,
            String topic,
            String groupId,
            boolean enableAutoCommit,
            MessageListener<String, String> handler) {
        return createKafkaListenerContainer(servers, topic, groupId, "latest", enableAutoCommit, handler);
    }

    public static KafkaMessageListenerContainer<String, String> createEarliestKafkaListenerContainer(
            String servers,
            String topic,
            String groupId,
            boolean enableAutoCommit,
            MessageListener<String, String> handler) {
        return createKafkaListenerContainer(servers, topic, groupId, "earliest", enableAutoCommit, handler);
    }

    public static KafkaMessageListenerContainer<String, String> createKafkaListenerContainer(
            String servers,
            String topic,
            String groupId,
            String autoOffsetReset,
            boolean enableAutoCommit,
            MessageListener<String, String> handler) {

        if (handler == null) {
            logger.warn("初始化kafka消费者时，没有设置handler");
            return null;
        }
        if (StringUtils.isBlank(servers)) {
            logger.warn("初始化kafka消费者时，没有设置servers");
            return null;
        }
        if (StringUtils.isBlank(topic)) {
            logger.warn("初始化kafka消费者时，没有设置topic");
            return null;
        }
        if (StringUtils.isBlank(groupId)) {
            logger.warn("初始化kafka消费者时，没有设置groupId");
            return null;
        }
        if (StringUtils.isBlank(autoOffsetReset)) {
            logger.warn("初始化kafka消费者时，没有设置autoOffsetReset");
            return null;
        }

        try {
            ContainerProperties containerProperties = new ContainerProperties(topic);
            containerProperties.setMessageListener(handler);
            if (enableAutoCommit == false) {
                //如果是手动commit，需要加上这句，否则spring kafka会帮忙自动commit
                containerProperties.setAckMode(ContainerProperties.AckMode.MANUAL);
            }

            Map<String, Object> props = consumerConfigs(servers, groupId, autoOffsetReset, enableAutoCommit);
            DefaultKafkaConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<>(props);

            KafkaMessageListenerContainer<String, String> container = new KafkaMessageListenerContainer<>(cf, containerProperties);

            return container;
        } catch (Exception e) {
            logger.info("初始化kafka消费者时报错", e);
        }
        return null;
    }
}
