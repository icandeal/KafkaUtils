package com.ycf;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * Created by sniper on 16-10-12.
 */
public class ConsumerThread extends Thread {

    private KafkaConsumer consumer;
    private ConsumerCallback callback;
    private Long cycle;

    public ConsumerThread(KafkaConsumer consumer, ConsumerCallback callback, long cycle){
        this.consumer = consumer;
        this.callback = callback;
        this.cycle = cycle;
    }

    @Override
    public void run() {
        while (true) {
            ConsumerRecords record = consumer.poll(cycle);
            callback.afterReceive(record);
        }
    }
}
