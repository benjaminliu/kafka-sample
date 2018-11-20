package com.ben.kafkasample;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;

@SpringBootApplication
public class KafkaSampleApplication implements InitializingBean, Runnable {

    public static void main(String[] args) {
        SpringApplication.run(KafkaSampleApplication.class, args);
    }

    private static Logger logger = LoggerFactory.getLogger(KafkaSampleApplication.class);


    @Autowired
    @Qualifier("myListener")
    private KafkaMessageListenerContainer<String, String> myListener;

    @Autowired
    @Qualifier("myProducer")
    private KafkaTemplate<String, String> myProducer;

    @Override
    public void afterPropertiesSet() throws Exception {

        myListener.start();

        new Thread(this).start();

    }

    @Override
    public void run() {
        for (int i = 0; i < 1000; i++) {
            myProducer.sendDefault("k" + i, "test" + i);
        }

        logger.info("send success");
    }
}
