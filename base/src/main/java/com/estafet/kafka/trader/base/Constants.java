package com.estafet.kafka.trader.base;

import com.estafet.kafka.trader.base.json.OrderSer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Created by Delcho Delov on 03.08.18.
 */
public final class Constants {
    public final static String[] SHARES = {"AAPL", "GOOG", "GM", "AMZN", "FB", "MSFT", "ITL"};
    public final static String TRADER_SERVERS = "localhost:9092,localhost:9093,localhost:9094";
    public final static String TOPIC_INCOMING_ORDERS = "incoming-orders";
    public final static List<String> SHARES_LIST = new ArrayList<>(Arrays.asList(SHARES));
    public static final BigDecimal PRICE_MAX_VALUE = BigDecimal.valueOf(Long.MAX_VALUE);

    public static Properties getProducerProperties(String clientId) {
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, TRADER_SERVERS);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 100);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, OrderSer.class);//"org.apache.kafka.common.serialization
        return props;
    }

}
