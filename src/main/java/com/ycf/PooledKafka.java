package com.ycf;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.Resource;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by sniper on 16-10-12.
 */
public class PooledKafka implements InitializingBean,Serializable {
    private Logger logger = Logger.getLogger(this.getClass());
    private GenericObjectPool<KafkaProducer> pool;
    private Resource configLocation;
    private Properties config;
    private KafkaProducer producer;

    public PooledKafka (){}

    public void initPooled(Properties config){
        this.config = config;
        pool = new GenericObjectPool<KafkaProducer>(new KafkaPooledProducerFactory(config));
        pool.setMaxIdle(config.containsKey("pool.maxIdle")? Integer.parseInt(config.getProperty("pool.maxIdle")) : 10);
        pool.setMinIdle(config.containsKey("pool.minIdle")? Integer.parseInt(config.getProperty("pool.minIdle")) : 3);
        pool.setMaxTotal(config.containsKey("pool.maxTotal")? Integer.parseInt(config.getProperty("pool.maxTotal")) : 500);
        pool.setMaxWaitMillis(config.containsKey("pool.maxWaitMillis")? Integer.parseInt(config.getProperty("pool.maxWaitMillis")) : 100000);
        if(!config.containsKey("key.serializer")) {
            config.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        }
        if(!config.containsKey("value.serializer")) {
            config.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        }
        if(!config.containsKey("key.deserializer")) {
            config.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        }
        if(!config.containsKey("value.deserializer")) {
            config.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        }
    }

    public boolean send(String topic, String key, Object value) {
        ProducerRecord<String, Object> record = new ProducerRecord<String, Object>(topic, key, value);
        KafkaProducer producer = null;
        if (pool != null) {
            try {
                producer = pool.borrowObject();
                producer.send(record);
<<<<<<< HEAD
                producer.flush();
=======
>>>>>>> 61d807a0dbdc35e5b29c4edd896649350dffa7da
            } catch (Exception e) {
                logger.error(e);
            } finally {
                try {
<<<<<<< HEAD
                    this.pool.returnObject(producer);
=======
                    pool.returnObject(producer);
>>>>>>> 61d807a0dbdc35e5b29c4edd896649350dffa7da
                } catch (Exception e) {
                    logger.error(e);
                }
            }
            return true;
        }
        return false;
    }

    public boolean send(String topic, Object value) {
        return send(topic, null, value);
    }

    public void receive(ConsumerCallback callback, String groupId, String topic, Long cycle, Integer numPartitions) {
        this.config.setProperty("group.id",groupId);
        KafkaConsumer<String, Object> consumer = new KafkaConsumer<String, Object>(this.config);
<<<<<<< HEAD
        List<String> topics = new ArrayList<String>();
        topics.add(topic);
        consumer.subscribe(topics);

        for (int i = 0; i < numPartitions; i++) {
            ConsumerThread thread = new ConsumerThread(consumer, callback, cycle);
            thread.run();
        }
    }

    public void receive(ConsumerCallback callback, String groupId, String topic, Long cycle) {
        receive(callback, groupId, topic, cycle, 1);
=======
        consumer.subscribe(topicList);
        ConsumerThread thread = new ConsumerThread(consumer,callback,cycle);
        thread.start();
>>>>>>> 61d807a0dbdc35e5b29c4edd896649350dffa7da
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
