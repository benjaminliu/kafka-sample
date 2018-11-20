package com.ben.kafkasample.config;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 * 线程池配置, queue为0， 由外部kafka作为队列 <br>
 *
 * @author: 刘恒 <br>
 * @date: 2018/11/19 <br>
 */
@Configuration
public class KafkaThreadPoolConfig {
    private Logger logger = LoggerFactory.getLogger(KafkaThreadPoolConfig.class);

    @Value("${task.executor.poolsize-min}")
    private Integer minPoolSize;

    @Value("${task.executor.poolsize-max}")
    private Integer maxPoolSize;

    @Value("${task.executor.keepalivesecond}")
    private Integer keepAliveSecond;

    public ThreadPoolTaskExecutor createNewThreadPool(String threadPrefix) {
        try {
            ThreadPoolTaskExecutor poolTaskExecutor = new ThreadPoolTaskExecutor();
            // 线程池所使用的缓冲队列,这里设为0， 不使用线程池的队列，而使用外部kafka队列
            poolTaskExecutor.setQueueCapacity(0);
            // 线程池维护线程的最少数量
            poolTaskExecutor.setCorePoolSize(minPoolSize);
            // 线程池维护线程的最大数量
            poolTaskExecutor.setMaxPoolSize(maxPoolSize);
            // 线程池维护线程所允许的空闲时间
            poolTaskExecutor.setKeepAliveSeconds(keepAliveSecond);
            if (StringUtils.isNotBlank(threadPrefix)) {
                poolTaskExecutor.setThreadNamePrefix(threadPrefix);
            }
            // init
            poolTaskExecutor.initialize();
            return poolTaskExecutor;
        } catch (Exception e) {
            logger.error("线程池初始化异常", e);
        }
        return null;
    }
}
