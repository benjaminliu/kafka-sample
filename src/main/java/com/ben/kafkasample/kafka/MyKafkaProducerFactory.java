package com.ben.kafkasample.kafka;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka消费者工厂 <br>
 *
 * @author: 刘恒 <br>
 * @date: 2018/11/19 <br>
 */
public class MyKafkaProducerFactory {
    private static Logger logger = LoggerFactory.getLogger(MyKafkaConsumerFactory.class);

    private static Map<String, Object> producerConfigs(String servers) {

        Map<String, Object> props = new HashMap<>();
        //连接地址
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        //重试次数，0为不启用重试机制
        props.put(ProducerConfig.RETRIES_CONFIG, 1);
        //控制批处理大小，单位为字节
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        //批量发送，延迟为1毫秒，启用该功能能有效减少生产者发送消息次数，从而提高并发量
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        //生产者可以使用的总内存字节来缓冲等待发送到服务器的记录
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 1024000);
        //键的序列化方式
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        //值的序列化方式
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return props;
    }


    public static ProducerFactory<String, String> createProducer(String servers) {
        try {
            if (StringUtils.isBlank(servers)) {
                logger.warn("初始化kafka生产者时，没有设置servers");
                return null;
            }

            return new DefaultKafkaProducerFactory<>(producerConfigs(servers));
        } catch (Exception e) {
            logger.error("初始化kafka生产者时出错", e);
        }
        return null;
    }
}
