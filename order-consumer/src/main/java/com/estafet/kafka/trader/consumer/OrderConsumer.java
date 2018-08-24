package com.estafet.kafka.trader.consumer;

import com.estafet.kafka.trader.base.Order;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

import static com.estafet.kafka.trader.base.Constants.TOPIC_INCOMING_ORDERS;
import static com.estafet.kafka.trader.base.Constants.TRADER_SERVERS;
import static com.estafet.kafka.trader.base.Constants.getProducerProperties;
import static com.estafet.kafka.trader.base.json.OrderDeserializer.getOrder;

/**
 * Created by Delcho Delov on 03.08.18.
 */
public class OrderConsumer {

    private static org.apache.log4j.Logger log = Logger.getLogger(OrderConsumer.class);

    public static void main(final String[] args) throws Exception {
        String log4jConfPath = "/home/ddelov/gitRepo/kafkaTest/order-consumer/src/main/resources/log4j.properties";
        PropertyConfigurator.configure(log4jConfPath);
        log.debug("consumer started");
        final Pattern compile = Pattern.compile(TOPIC_INCOMING_ORDERS + ".*");

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "sh-trader-application");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, TRADER_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "sh-trader-gr1");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "10000");//10 sec
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(compile);

        UnmatchedRequests unmatched = new UnmatchedRequests();
        ObjectMapper mapper = new ObjectMapper();
        //shutdown hook - put back unmatched records to kafka
        AtomicBoolean shutdownRequested = new AtomicBoolean(false);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            //TODO wait if/until unmatched processing is done
            //put back unmatched records to kafka
            shutdownRequested.set(true);
            try (Producer<String, Order> producer = new KafkaProducer<>(getProducerProperties("consumer-unconsumed"));) {
                for (Order order : unmatched.getUnmatchedRequests()) {
                    producer.send(new ProducerRecord<String, Order>(TOPIC_INCOMING_ORDERS + order.symbol, order));
                    producer.flush();
                }
            }
            log.info("Consumer finished. Unmatched orders are back to Kafka");
        }));
        while (!shutdownRequested.get()) {
            log.trace("Poll for incoming orders...");
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10l));
            log.debug(records.count() > 0 ? "Found " + records.count() + " new records." : "No new records");
            for (ConsumerRecord<String, String> record : records) {
                final JsonNode node = mapper.readValue(record.value(), JsonNode.class);
                final Order order = getOrder(node);
                unmatched.add(order);
            }
            log.trace("start matching..");
            unmatched.startMatching();
            log.trace("matching finished");
        }
    }
}
