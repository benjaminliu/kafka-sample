package com.ben.kafkasample.kafka;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.ProducerListener;

/**
 * 默认的生产者监听器 <br>
 *
 * @author: 刘恒 <br>
 * @date: 2018/11/19 <br>
 */
public class DefaultProducerListerner implements ProducerListener<String, String> {
    private static Logger logger = LoggerFactory.getLogger(DefaultProducerListerner.class);

    @Override
    public void onSuccess(String topic, Integer partition, String key, String value, RecordMetadata recordMetadata) {
        logger.info("kafka send success, topic: {}, key: {}, offset: {}", topic, key, recordMetadata.offset());
    }

    @Override
    public void onError(String topic, Integer partition, String key, String value, Exception exception) {
        logger.info("Kafka send failed, topic: {}, key: {}, reason: {}", topic, key, exception.getMessage());
    }
}
