package com.estafet.kafka.trader.producer;

import com.estafet.kafka.trader.base.Order;
import com.estafet.kafka.trader.base.OrderOperation;
import com.estafet.kafka.trader.base.OrderType;
import com.estafet.kafka.trader.base.json.OrderSer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.math.BigDecimal;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import static com.estafet.kafka.trader.base.Constants.*;

/**
 * Created by Delcho Delov on 01.08.18.
 */
public class GenerateTradeRequests {


    public static void main(String[] args) {
        for (String arg : args) {
            System.out.println("arg = " + arg);
        }
        int generetedTradeRequests = 20;
        long initialReqId = 100L;
        long initialUserId = 4000L;
        if(args!=null && args.length>2){
            generetedTradeRequests = Integer.parseInt(args[0]);
            initialReqId = Long.parseLong(args[1]);
            initialUserId = Long.parseLong(args[2]);
        }
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, TRADER_SERVERS);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, OrderSer.class/*"org.apache.kafka.common.serialization
        .StringSerializer"*/);
        //fake data
        final AtomicLong orderSeq = new AtomicLong(initialReqId);
        final AtomicLong userSeq = new AtomicLong(initialUserId);
        final Random rnd = new Random(System.nanoTime());
        try (Producer<String, Order> producer = new KafkaProducer<>(props);) {
            for (int i = 0; i < generetedTradeRequests; i++) {
                final String symbol = SHARES[rnd.nextInt(SHARES.length)];
                OrderOperation operation = OrderOperation.values()[rnd.nextInt(2)];
                BigDecimal price = BigDecimal.valueOf(100 + rnd.nextDouble());
                Order order = new Order(orderSeq.getAndIncrement(), userSeq.getAndIncrement(), symbol, operation,
                        rnd.nextInt(200), price, OrderType.Limit);

                producer.send(new ProducerRecord<String, Order>(TOPIC_INCOMING_ORDERS + symbol, order));
                producer.flush();
            }
        }
    }
}
