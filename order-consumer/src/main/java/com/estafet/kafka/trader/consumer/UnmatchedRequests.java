package com.estafet.kafka.trader.consumer;

import com.estafet.kafka.trader.base.Constants;
import com.estafet.kafka.trader.base.Order;
import com.estafet.kafka.trader.base.comparators.PriceComparator;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Delcho Delov on 21.08.18.
 */
public class UnmatchedRequests {
    //sort order by price for all operations(Buy, sell,..)
//    private TreeSet<Order> sortedS2 = new TreeSet<>(new PriceComparator());
    private Map<String, SortedSet<Order>> activeRequests = new ConcurrentHashMap<>();
    private Set<String> modifiedSymbols = new HashSet<>();
    private static Logger log = LogManager.getLogger(UnmatchedRequests.class);

    public boolean add(Order order) {
        if (order == null) {
            log.error("order is null");
            throw new NullPointerException();
        }
        modifiedSymbols.add(order.symbol);
        return getOrders(order.symbol).add(order);
    }

    SortedSet<Order> getOrders(final String symbol) {
        if (symbol == null || !Constants.SHARES_LIST.contains(symbol)) {
            log.error("symbol is " + symbol);
            throw new NullPointerException();
        }
        activeRequests.putIfAbsent(symbol, new TreeSet<Order>(new PriceComparator()));
        return activeRequests.get(symbol);
    }

    public Set<Order> getUnmatchedRequests() {
        Set<Order> result = new HashSet<>();
        for (SortedSet<Order> orders : activeRequests.values()) {
            result.addAll(orders);
        }
        return result;
    }

//    public void startMatching() {
//        final Iterator<String> iterator = modifiedSymbols.iterator();
//        while (iterator.hasNext()) {
//            final String symbol = iterator.next();
//            iterator.remove();
//            final OrderMatcher orderMatcher = new OrderMatcher(getOrders(symbol));
//            log.info("matching started for " + symbol);
//            final Integer deals = orderMatcher.proceed();
//            log.info("deals made :" + deals);
//            final String ordersTable = printOrdersTable(getOrders(symbol));
//            log.info(ordersTable);
//        }
//    }

    public String printOrdersTable(/*SortedSet<Order> orders*/) {
        StringBuilder sb = new StringBuilder();
        sb.append('\n').append(Order.printHeader()).append('\n');
        for (String symbol : activeRequests.keySet()) {
            for (Order order : getOrders(symbol)) {
                sb.append(order.printAsTable()).append('\n');
            }
        }
        return sb.toString();
    }

//    public static void matchInTransaction(ConsumerRecord<String, String> record) throws IOException {
//
//        final Properties producerProperties = getProducerProperties("transaction-1-matcher");
//        producerProperties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transOne");//TODO transactional id
//        KafkaProducer producer = new KafkaProducer(producerProperties);
//        producer.initTransactions();
//
//        final ObjectMapper mapper = new ObjectMapper();
//        final JsonNode node = mapper.readValue(record.value(), JsonNode.class);
//        final Order order = getOrder(node);
//        final OrderMatcher orderMatcher = new OrderMatcher(getOrders(order.symbol));
//        {
//            producer.beginTransaction();
//            try {
//                orderMatcher.match(order);
//            } catch (OrderException ignore) {
//                log.warn(ignore.getMessage());
//            }
////            FinishedDeal deal =null;//TODO
//            //TODO commit record offset
//            record. consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
//            producer.commitTransaction();
//        }
//    }
}
