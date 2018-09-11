package com.estafet.kafka.trader.consumer;

import com.estafet.kafka.trader.base.Order;
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
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

import static com.estafet.kafka.trader.base.Constants.*;
import static com.estafet.kafka.trader.base.json.OrderDeserializer.getOrder;

/**
 * Created by Delcho Delov on 03.08.18.
 */
public class OrderConsumer {

    private static org.apache.log4j.Logger log = Logger.getLogger(OrderConsumer.class);

    public static void main(final String[] args) throws Exception {
        String log4jConfPath = "/home/ddelov/gitRepo/kafkaTest/order-consumer/src/main/resources/log4j.properties";
        PropertyConfigurator.configure(log4jConfPath);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "sh-trader-application");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, TRADER_SERVERS);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "sh-trader-gr1");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, StringDeserializer.class);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        final Pattern compile = Pattern.compile(TOPIC_INCOMING_ORDERS + ".*");
        consumer.subscribe(compile);

        UnmatchedRequests unmatched = new UnmatchedRequests();
//1        ObjectMapper mapper = new ObjectMapper();
        //shutdown hook - put back unmatched records to kafka
        AtomicBoolean shutdownRequested = new AtomicBoolean(false);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            //TODO wait if/until unmatched processing is done
            //put back unmatched records to kafka
            shutdownRequested.set(true);
            try (Producer<String, Order> producer = new KafkaProducer<>(getProducerProperties("consumer-unconsumed"));) {
                for (Order order : unmatched.getUnmatchedRequests()) {
                    producer.send(createSortedProducerRecord(order));
                    producer.flush();
                }
            }
            log.info("Consumer finished. Unmatched orders are back to Kafka");
            final String ordersTable = unmatched.printOrdersTable();
            log.info(ordersTable);

        }));
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        log.debug("consumer started");
        try {
            while (!shutdownRequested.get()) {
                log.trace("Poll for incoming orders...");
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(POLL_EVERY_X_SECONDS));
                log.debug(records.count() > 0 ? "Found " + records.count() +
                        (records.count() == 1 ? " new record." : " new records.")
                        : "No new records");
//                for (TopicPartition partition : records.partitions()) {
//                    partition.equals()
//                    List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
//                    for (ConsumerRecord<String, String> record : partitionRecords) {
//                        System.out.println(record.offset() + ": " + record.value());
//                    }
//                    long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
//                    consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
//                }
                for (ConsumerRecord<String, String> record : records) {
                    final Properties producerProperties = getProducerProperties("transaction-1-matcher");
                    producerProperties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transOne");//TODO transactional id
                    KafkaProducer producer = new KafkaProducer(producerProperties);
                    producer.initTransactions();

                    final ObjectMapper mapper = new ObjectMapper();
                    final JsonNode node = mapper.readValue(record.value(), JsonNode.class);
                    final Order order = getOrder(node);
                    final OrderMatcher orderMatcher = new OrderMatcher(unmatched.getOrders(order.symbol));
                    {
                        producer.beginTransaction();
                        final long startN = System.nanoTime();
                        try {
                            orderMatcher.match(order);
                        } catch (OrderException ignore) {
                            log.warn(ignore.getMessage());
                        }
                        final long endN = System.nanoTime();
                        offsets.put(new TopicPartition(record.topic(), record.partition()),
                                new OffsetAndMetadata(record.offset()+1));
                        consumer.commitSync(offsets);
                        producer.commitTransaction();
                        log.debug(displayElapsedTime(endN-startN, "matching"));
                    }

                }
            }
        }finally {
            consumer.close();
        }
    }

}
