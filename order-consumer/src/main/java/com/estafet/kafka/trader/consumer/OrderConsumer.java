package com.estafet.kafka.trader.consumer;

import com.estafet.kafka.trader.base.Order;
import com.estafet.kafka.trader.base.UnmatchedRequests;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.log4j.PropertyConfigurator;

import java.time.Duration;
import java.util.Properties;
import java.util.regex.Pattern;

import static com.estafet.kafka.trader.base.Constants.TOPIC_INCOMING_ORDERS;
import static com.estafet.kafka.trader.base.Constants.TRADER_SERVERS;
import static com.estafet.kafka.trader.base.json.OrderDeserializer.getOrder;

/**
 * Created by Delcho Delov on 03.08.18.
 */
public class OrderConsumer {


    public static void main(final String[] args) throws Exception {
        String log4jConfPath = "/home/ddelov/gitRepo/kafkaTest/base/src/main/resources/log4j.properties";
        PropertyConfigurator.configure(log4jConfPath);

        final Pattern compile = Pattern.compile(TOPIC_INCOMING_ORDERS + ".*");
//        System.out.println("matcher.find() = " + compile.matcher("AAPL").find());
//        for (String share : SHARES) {
//            final java.util.regex.OrderConsumer matcher = compile.matcher(TOPIC_INCOMING_ORDERS + share);
//            System.out.println("matcher.find() = " + matcher.find());
//        }

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

        UnmatchedRequests sortedS2 = new UnmatchedRequests();
        ObjectMapper mapper = new ObjectMapper();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10l));
            for (ConsumerRecord<String, String> record : records) {
                final JsonNode node = mapper.readValue(record.value(), JsonNode.class);
                final Order order= getOrder(node);
                sortedS2.add(order);
            }
            //start matching - sortedS2
            sortedS2.startMatching();
        }
    }
}
