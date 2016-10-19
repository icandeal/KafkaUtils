package com.ycf;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;

/**
 * Created by sniper on 16-10-12.
 */
public class KafkaTest {
    private PooledKafka pooledKafka;
    @Before
    public void beforeTest() throws IOException {
        pooledKafka = new PooledKafka();
        Properties properties = new Properties();
        properties.load(this.getClass().getResourceAsStream("/properties/config.properties"));
        pooledKafka.initPooled(properties);
    }

    @Test
    public void doSend(){
        Assert.assertTrue(pooledKafka.send("test", "fff", "cccc"));
    }

    @Test
    public void doReceive(){
        pooledKafka.receive(new ConsumerCallback() {
            public void afterReceive(ConsumerRecords records) {
                Iterator<ConsumerRecord<String, Object>> it = records.iterator();
                while (it.hasNext()) {
                    ConsumerRecord<String, Object> record1 = it.next();
                    System.out.println("key:"+record1.key()
                            +" value:"+record1.value()
                            + " partition:"+record1.partition());
                }
            }
        },"test", Arrays.asList("test"), 1000l);
        Assert.assertTrue(true);
    }
}
