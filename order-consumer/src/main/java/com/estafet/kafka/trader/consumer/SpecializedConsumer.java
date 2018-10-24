package com.estafet.kafka.trader.consumer;

import com.estafet.kafka.trader.base.Order;
import com.estafet.kafka.trader.base.OrderOperation;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.estafet.kafka.trader.base.Constants.*;
import static com.estafet.kafka.trader.base.json.OrderDeserializer.getOrder;

/**
 * Created by Delcho Delov on 13.09.18.
 */
public class SpecializedConsumer {
    public boolean shutdownRequested = false;

    private final SeparatedOrderMatcher orderMatcher;
    private final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
    private final KafkaConsumer<String, String> consumer;
    private final TopicPartition partition;
    private final AtomicLong transIdSeq = new AtomicLong();
    private static Logger log = Logger.getLogger(SpecializedConsumer.class);

    public SpecializedConsumer(String groupId, SeparatedOrderMatcher orderMatcher, String symbol,
                               OrderOperation operation, int offset) {
        this.orderMatcher = orderMatcher;
        consumer = new KafkaConsumer<>(getConsumerProperties(groupId));
        partition = new TopicPartition(TOPIC_INCOMING_ORDERS + symbol, PARTITIONS_IN_TOPIC/2* operation.ordinal() + offset);
        consumer.assign(Arrays.asList(partition));
    }

    public void infinitePoll(){
        log.debug("consumer started");
        try {
            while (!shutdownRequested) {
                log.trace("Poll for incoming orders...");
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(POLL_EVERY_X_SECONDS));
                int count = records.count();
                log.debug(count > 0 ? "Found " + count +
                        (count == 1 ? " new record." : " new records.")
                        : "No new records");
                final long startN = System.nanoTime();
                for (ConsumerRecord<String, String> record : records) {
                    final String transId = Thread.currentThread().getName() + transIdSeq.getAndIncrement();
                    final Properties producerProperties = getProducerProperties(transId +"-matcher");
                    producerProperties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transId);
                    KafkaProducer producer = new KafkaProducer(producerProperties);
                    producer.initTransactions();

                    final ObjectMapper mapper = new ObjectMapper();
                    try {
                        final JsonNode node = mapper.readValue(record.value(), JsonNode.class);
                        final Order order = getOrder(node);
                        {
                            producer.beginTransaction();
                            orderMatcher.add(order);
                            offsets.put(partition, new OffsetAndMetadata(record.offset() + 1));
                            consumer.commitSync(offsets);
                            producer.commitTransaction();
                        }
                    }catch (IOException ignored){
                        log.warn("Could not convert json to Order", ignored);
                    }

                }
                final long endN = System.nanoTime();
                log.debug(displayElapsedTime(endN - startN, "Processed " + count + " records "));
            }
        } finally {
            consumer.close();
        }
    }

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
            shutdownRequested.set(true);
            //wait if/until unmatched processing is done
            try {
                matcher.awaitTermination();
            } catch (Exception ignored) {
                log.error(ignored.getMessage(), ignored);
            }
            //put back unmatched records to kafka
            try (Producer<String, Order> prB = new KafkaProducer<>(getProducerProperties("consumer-unconsumed-b"));
                 Producer<String, Order> prS = new KafkaProducer<>(getProducerProperties("consumer-unconsumed-s"))) {
                final ForkJoinTask<?> task1 = ForkJoinPool.commonPool().submit(() -> {
                    for (Order order : matcher.getBuyOrders()) {
                        prB.send(createSortedProducerRecord(order));
                        prB.flush();
                    }
                });
                final ForkJoinTask<?> task = ForkJoinPool.commonPool().submit(() -> {
                    for (Order order : matcher.getSellOrders()) {
                        prS.send(createSortedProducerRecord(order));
                        prS.flush();
                    }
                });
                try {
                    task1.get(10L, TimeUnit.SECONDS);
                    task.get(10L, TimeUnit.SECONDS);
                }catch (Exception ignored){
                    log.error(ignored.getMessage(), ignored);
                }
            }
            log.info("Consumer finished. Unmatched orders are back to Kafka");

        }));
        List<ForkJoinTask<?>>  consumersTasks = new ArrayList<>(PARTITIONS_IN_TOPIC);
        for (int offset=0;offset<PARTITIONS_IN_TOPIC;++offset) {
            final SpecializedConsumer buyConsumer = new SpecializedConsumer(groupId, matcher, symbol, OrderOperation.BUY, offset);
            final SpecializedConsumer sellConsumer = new SpecializedConsumer(groupId, matcher, symbol, OrderOperation.SELL, offset);

            consumersTasks.add(ForkJoinPool.commonPool().submit(buyConsumer::infinitePoll));
            consumersTasks.add(ForkJoinPool.commonPool().submit(sellConsumer::infinitePoll));

        }
        for (ForkJoinTask<?> task : consumersTasks) {
            task.get();
        }
//        subB.get();
//        subS.get();
        log.info("Program finished");

    }

}
