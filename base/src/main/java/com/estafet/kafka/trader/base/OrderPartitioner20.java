package com.estafet.kafka.trader.base;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;

/**
 * Created by Delcho Delov on 24.10.18
 */
public class OrderPartitioner20 implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        if(value instanceof Order){
            final Order order  = (Order) value;
            return getSlot(numPartitions, order.price.doubleValue(), order.operation);
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
    protected int getSlot(int numPartitions, double price, OrderOperation operation){
        if(numPartitions<2){
            return 0;
        }
        final int priceInCents = (int)(price * 100);
        final int halfPartitions = numPartitions/2;
        final int lowerBits = priceInCents % halfPartitions;//up to halfPartitions
        final int desiredHalf = operation.ordinal();//0(lower) or 1(upper)
        final int slot = desiredHalf*halfPartitions + lowerBits;
        return slot>=numPartitions?numPartitions-1:slot;
    }

}
