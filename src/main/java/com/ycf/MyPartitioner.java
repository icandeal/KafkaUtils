package com.ycf;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by sniper on 16-9-14.
 */
public class MyPartitioner implements Partitioner,Serializable {
    public int partition(String s, Object o, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {
        System.out.println(cluster.partitionCountForTopic(s));
        return o.hashCode()%cluster.partitionCountForTopic(s);
    }

    public void close() {
        System.out.println("close");
    }

    public void configure(Map<String, ?> map) {
        System.out.println(map);
    }
}
