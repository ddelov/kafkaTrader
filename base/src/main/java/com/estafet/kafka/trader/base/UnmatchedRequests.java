package com.estafet.kafka.trader.base;

import com.estafet.kafka.trader.base.comparators.PriceComparator;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

/**
 * Created by Delcho Delov on 21.08.18.
 */
public class UnmatchedRequests {
    //sort order by price for all operations(Buy, sell,..)
//    private TreeSet<Order> sortedS2 = new TreeSet<>(new PriceComparator());
    private Map<String, SortedSet<Order>> activeRequests = new ConcurrentHashMap<>();
    private Set<String> modifiedSymbols = new HashSet<>();
    private static Logger log = Logger.getLogger(UnmatchedRequests.class);

    public boolean add(Order order) {
        if (order == null) {
            log.error("order is null");
            throw new NullPointerException();
        }
        modifiedSymbols.add(order.symbol);
        return getOrders(order.symbol).add(order);
    }

    private SortedSet<Order> getOrders(final String symbol) {
        if (symbol == null || !Constants.SHARES_LIST.contains(symbol)) {
            log.error("symbol is "+symbol);
            throw new NullPointerException();
        }
        activeRequests.putIfAbsent(symbol, new TreeSet<Order>(new PriceComparator()));
        return activeRequests.get(symbol);
    }

    public void startMatching() throws ExecutionException, InterruptedException {
        final Iterator<String> iterator = modifiedSymbols.iterator();
        while (iterator.hasNext()) {
            final String symbol = iterator.next();
            iterator.remove();
            final OrderMatcher orderMatcher = new OrderMatcher(getOrders(symbol));
            log.info("proceed started for " + symbol);
            final Integer deals = orderMatcher.proceed();
            log.info("deals made :" + deals);
        }
//        System.out.println("matching finished");
    }

}
