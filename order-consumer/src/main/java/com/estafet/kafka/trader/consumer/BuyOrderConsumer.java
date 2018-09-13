package com.estafet.kafka.trader.consumer;

import com.estafet.kafka.trader.base.Order;
import com.estafet.kafka.trader.base.OrderOperation;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.estafet.kafka.trader.base.Constants.*;

/**
 * Created by Delcho Delov on 03.08.18.
 */
public class BuyOrderConsumer {

    private static Logger log = Logger.getLogger(BuyOrderConsumer.class);

    public static void main(final String[] args) throws Exception {
        if (args == null || args.length < 2 || !SHARES_LIST.contains(args[1])) {
            System.out.println("please provide parameters : <groupId> <symbol>");
            System.out.println("\tgroupId is a string identifying consumer group");
            System.out.println("\tsymbol can be one of " + String.join(", ", SHARES_LIST));
            System.exit(0);
        }

        final String groupId = args[0];
        final String symbol = args[1];

        String log4jConfPath = "/home/ddelov/gitRepo/kafkaTest/order-consumer/src/main/resources/log4j.properties";
        PropertyConfigurator.configure(log4jConfPath);

        //shutdown hook - put back unmatched records to kafka
        AtomicBoolean shutdownRequested = new AtomicBoolean(false);
        SeparatedOrderMatcher matcher = new SeparatedOrderMatcher();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            //TODO wait if/until unmatched processing is done
            try {
                matcher.awaitTermination();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            //put back unmatched records to kafka
            shutdownRequested.set(true);
            try (Producer<String, Order> producer = new KafkaProducer<>(getProducerProperties("consumer-unconsumed"));) {
                ForkJoinPool.commonPool().execute(() -> {
                    for (Order order : matcher.getBuyOrders()) {
                        producer.send(createSortedProducerRecord(order));
                        producer.flush();
                    }
                });
                ForkJoinPool.commonPool().execute(() -> {
                    for (Order order : matcher.getSellOrders()) {
                        producer.send(createSortedProducerRecord(order));
                        producer.flush();
                    }
                });
            }
            log.info("Consumer finished. Unmatched orders are back to Kafka");
//            final String ordersTable = unmatched.printOrdersTable();
//            log.info(ordersTable);

        }));
        final SpecializedConsumer buyConsumer = new SpecializedConsumer(matcher, symbol, OrderOperation.BUY, log);
        ForkJoinPool.commonPool().submit(buyConsumer::infinitePoll);
        final SpecializedConsumer sellConsumer = new SpecializedConsumer(matcher, symbol, OrderOperation.SELL, log);
        ForkJoinPool.commonPool().submit(sellConsumer::infinitePoll);

//        KafkaConsumer<String, String> sellConsumer = new KafkaConsumer<>(getConsumerProperties());
//        TopicPartition sellPart = new TopicPartition(TOPIC_INCOMING_ORDERS + symbol, OrderOperation.SELL.ordinal());
//        sellConsumer.assign(Arrays.asList(sellPart));


    }

}
