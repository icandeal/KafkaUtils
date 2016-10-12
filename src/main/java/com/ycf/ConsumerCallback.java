package com.ycf;

import org.apache.kafka.clients.consumer.ConsumerRecords;

/**
 * Created by sniper on 16-10-12.
 */
public interface ConsumerCallback {
    void afterReceive(ConsumerRecords records);
}
