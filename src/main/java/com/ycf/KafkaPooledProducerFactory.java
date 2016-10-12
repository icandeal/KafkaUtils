package com.ycf;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Properties;

/**
 * Created by sniper on 16-10-12.
 */
public class KafkaPooledProducerFactory extends BasePooledObjectFactory<KafkaProducer> {
    private Properties properties;

    public KafkaPooledProducerFactory (Properties properties){
        this.properties = properties;
    }

    public KafkaProducer create() throws Exception {
        return new KafkaProducer(properties);
    }

    public PooledObject<KafkaProducer> wrap(KafkaProducer kafkaProducer) {
        return new DefaultPooledObject<KafkaProducer>(kafkaProducer);
    }

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }
}
