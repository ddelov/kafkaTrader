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
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
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
    private final Logger log;

    public SpecializedConsumer(SeparatedOrderMatcher orderMatcher, String symbol, OrderOperation operation, Logger log) {
        this.orderMatcher = orderMatcher;
        consumer = new KafkaConsumer<>(getConsumerProperties("group" + symbol));
        this.log = log;
        partition = new TopicPartition(TOPIC_INCOMING_ORDERS + symbol, operation.ordinal());
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
//                for (TopicPartition partition : records.partitions()) {
//                    partition.equals()
//                    List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
//                    for (ConsumerRecord<String, String> record : partitionRecords) {
//                        System.out.println(record.offset() + ": " + record.value());
//                    }
//                    long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
//                    consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
//                }
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

}
