package com.estafet.kafka.trader.consumer;

import com.estafet.kafka.trader.base.Order;
import com.estafet.kafka.trader.base.OrderOperation;
import com.estafet.kafka.trader.base.comparators.PriceComparator;
import com.estafet.kafka.trader.base.exception.OrderException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.estafet.kafka.trader.base.Constants.*;
import static com.estafet.kafka.trader.base.json.OrderDeserializer.getOrder;

/**
 * Created by Delcho Delov on 03.08.18.
 */
public class BuyOrderConsumer {

    private static Logger log = Logger.getLogger(BuyOrderConsumer.class);

    public static void main(final String[] args) throws Exception {
        final SortedSet<Order> buyOrders = new TreeSet<Order>(new PriceComparator());
        final SortedSet<Order> sellOrders = new TreeSet<Order>(new PriceComparator());
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

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "sh-trader-application");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, TRADER_SERVERS);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, StringDeserializer.class);

        KafkaConsumer<String, String> buyConsumer = new KafkaConsumer<>(props);
        TopicPartition buyPart = new TopicPartition(TOPIC_INCOMING_ORDERS + symbol, OrderOperation.BUY.ordinal());
        buyConsumer.assign(Arrays.asList(buyPart));

        KafkaConsumer<String, String> sellConsumer = new KafkaConsumer<>(props);
        TopicPartition sellPart = new TopicPartition(TOPIC_INCOMING_ORDERS + symbol, OrderOperation.SELL.ordinal());
        sellConsumer.assign(Arrays.asList(sellPart));

//        UnmatchedRequests unmatched = new UnmatchedRequests();
//1        ObjectMapper mapper = new ObjectMapper();
        //shutdown hook - put back unmatched records to kafka
        AtomicBoolean shutdownRequested = new AtomicBoolean(false);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            //TODO wait if/until unmatched processing is done
            //put back unmatched records to kafka
            shutdownRequested.set(true);
            try (Producer<String, Order> producer = new KafkaProducer<>(getProducerProperties("consumer-unconsumed"));) {
                ForkJoinPool.commonPool().execute(() -> {
                    for (Order order : buyOrders) {
                        producer.send(createSortedProducerRecord(order));
                        producer.flush();
                    }
                });
                ForkJoinPool.commonPool().execute(() -> {
                for (Order order : sellOrders) {
                    producer.send(createSortedProducerRecord(order));
                    producer.flush();
                }
                });
            }
            log.info("Consumer finished. Unmatched orders are back to Kafka");
//            final String ordersTable = unmatched.printOrdersTable();
//            log.info(ordersTable);

        }));

        log.debug("consumer started");
        try {
            while (!shutdownRequested.get()) {
                pollIncoming(offsets);
            }
        } finally {
            buyConsumer.close();
            sellConsumer.close();
        }
    }

    private static void matchIncomingBuyOrders(TopicPartition buyPartition,
                                              KafkaConsumer<String, String> buyConsumer,
                                              final SortedSet<Order> sellOrders) throws java.io.IOException {
        log.trace("Poll for incoming orders...");
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        ConsumerRecords<String, String> records = buyConsumer.poll(Duration.ofSeconds(POLL_EVERY_X_SECONDS));
        log.debug(records.count() > 0 ? "Found " + records.count() +
                (records.count() == 1 ? " new record." : " new records.")
                : "No new records");
        for (ConsumerRecord<String, String> record : records) {
            final Properties producerProperties = getProducerProperties("buy-1-matcher");
            producerProperties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transBuy");//TODO transactional id
            KafkaProducer producer = new KafkaProducer(producerProperties);
            producer.initTransactions();

            final ObjectMapper mapper = new ObjectMapper();
            final JsonNode node = mapper.readValue(record.value(), JsonNode.class);
            final Order order = getOrder(node);
            final OrderMatcher orderMatcher = new OrderMatcher(sellOrders);
            {
                producer.beginTransaction();
                final long startN = System.nanoTime();
                try {
                    orderMatcher.match(order);
                } catch (OrderException ignore) {
                    log.warn(ignore.getMessage());
                }
                final long endN = System.nanoTime();
                offsets.put(partition, new OffsetAndMetadata(record.offset() + 1));
                consumer.commitSync(offsets);
                producer.commitTransaction();
                log.debug(displayElapsedTime(endN - startN, "matching"));
            }

        }
    }

}
