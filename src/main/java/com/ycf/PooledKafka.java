package com.ycf;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.Resource;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

/**
 * Created by sniper on 16-10-12.
 */
public class PooledKafka implements InitializingBean {
    private Logger logger = Logger.getLogger(this.getClass());

    private GenericObjectPool<KafkaProducer> pool;
    private Resource configLocation;
    private Properties config;

    public PooledKafka (){}

    public void initPooled(Properties config){
        this.config = config;
        pool = new GenericObjectPool<KafkaProducer>(new KafkaPooledProducerFactory(config));
        pool.setMaxIdle(config.containsKey("pool.maxIdle")? Integer.parseInt(config.getProperty("pool.maxIdle")) : 10);
        pool.setMinIdle(config.containsKey("pool.minIdle")? Integer.parseInt(config.getProperty("pool.minIdle")) : 3);
        pool.setMaxTotal(config.containsKey("pool.maxTotal")? Integer.parseInt(config.getProperty("pool.maxTotal")) : 500);
        pool.setMaxWaitMillis(config.containsKey("pool.maxWaitMillis")? Integer.parseInt(config.getProperty("pool.maxWaitMillis")) : 100000);
    }

    public boolean send(String topic, String key, Object value) {
        ProducerRecord<String, Object> record = new ProducerRecord<String, Object>(topic, key, value);

        if (pool != null) {
            try {
                pool.borrowObject().send(record);
            } catch (Exception e) {
                e.printStackTrace();
            }
            return true;
        }
        return false;
    }

    public boolean send(String topic, Object value) {
        ProducerRecord<String, Object> record = new ProducerRecord<String, Object>(topic, value);

        if (pool != null) {
            try {
                pool.borrowObject().send(record);
            } catch (Exception e) {
                e.printStackTrace();
            }
            return true;
        }
        return false;
    }

    public void receive(ConsumerCallback callback, String groupId, List<String> topicList, Long cycle) {
        this.config.setProperty("group.id",groupId);
        KafkaConsumer<String, Object> consumer = new KafkaConsumer<String, Object>(this.config);
        consumer.subscribe(topicList);
        ConsumerThread thread = new ConsumerThread(consumer,callback,cycle);
        thread.run();
    }

    public Resource getConfigLocation() {
        return configLocation;
    }

    public void setConfigLocation(Resource configLocation) {
        this.configLocation = configLocation;
    }

    public void afterPropertiesSet() throws Exception {
        try {
            Properties config = new Properties();
            config.load(configLocation.getInputStream());
            initPooled(config);
        } catch (IOException e)  {
            logger.error("Properties Load Error !\n",e);
        }
    }
}
