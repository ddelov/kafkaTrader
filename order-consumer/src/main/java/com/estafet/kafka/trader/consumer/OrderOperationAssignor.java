package com.estafet.kafka.trader.consumer;

import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Map;

/**
 * Created by Delcho Delov on 12.09.18.
 */
public class OrderOperationAssignor extends RangeAssignor {
    @Override
    public Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic, Map<String,
            Subscription> subscriptions) {
        return super.assign(partitionsPerTopic, subscriptions);
    }

}
