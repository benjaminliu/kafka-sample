package com.ben.kafkasample.handler;

import com.ben.kafkasample.config.KafkaThreadPoolConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.task.TaskRejectedException;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import java.util.concurrent.Semaphore;

/**
 * 消息消费者处理程序 <br>
 *
 * @author: 刘恒 <br>
 * @date: 2018/11/19 <br>
 */
@Component
public class MyKafkaHandler implements AcknowledgingMessageListener<String, String>, InitializingBean {
    private static Logger logger = LoggerFactory.getLogger(MyKafkaHandler.class);

    @Autowired
    private KafkaThreadPoolConfig kafkaThreadPoolConfig;

    private ThreadPoolTaskExecutor threadPoolTaskExecutor;

    private volatile Semaphore semaphore;

    @Override
    public void onMessage(ConsumerRecord<String, String> consumerRecord, Acknowledgment acknowledgment) {

        try {
            semaphore.acquire();
        } catch (InterruptedException e) {
            logger.info("semaphore.acquire()时报错", e);
            //获取失败， 不确认，kafka会重试
            return;
        }

        boolean hasError;
        do {
            hasError = false;
            try {
                if (StringUtils.isNotBlank(consumerRecord.value())) {
                    logger.info("active threads: {} ", threadPoolTaskExecutor.getActiveCount());

                    threadPoolTaskExecutor.execute(() -> {
                        try {
                            //process value
                            logger.info(consumerRecord.value());
                        } catch (Exception e) {
                            logger.error("解析ticket valid报文时出错", e);
                        } finally {
                            //先确认，再 release
                            acknowledgment.acknowledge();
                            semaphore.release();
                        }
                    });
                }
            } catch (TaskRejectedException e) {
                //当发生TaskRejectedException的时候， retry
                hasError = true;
                logger.error("向线程池里发送解析任务时出错: " + e.getMessage());
            }
        } while (hasError == true);
    }

    @Override
    public void afterPropertiesSet() throws Exception {

        threadPoolTaskExecutor = kafkaThreadPoolConfig.createNewThreadPool("my-thread-");
        //比maxPoolSize小1， avoid TaskRejectedException
        semaphore = new Semaphore(threadPoolTaskExecutor.getMaxPoolSize() - 1);
    }
}
