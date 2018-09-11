package com.estafet.kafka.trader.base;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;

/**
 * Created by Delcho Delov on 11.09.18.
 */
public class TradeOperationPartitioner implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        if(value instanceof Order){
            final Order order  = (Order) value;
            final int desiredNb = order.operation.ordinal();
            return desiredNb>=numPartitions?numPartitions-1:desiredNb;
        }else{
            return 0;
        }
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
