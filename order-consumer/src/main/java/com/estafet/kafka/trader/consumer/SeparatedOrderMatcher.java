package com.estafet.kafka.trader.consumer;

import com.estafet.kafka.trader.base.FinishedDeal;
import com.estafet.kafka.trader.base.Order;
import com.estafet.kafka.trader.base.OrderOperation;
import com.estafet.kafka.trader.base.comparators.PriceComparator;
import com.estafet.kafka.trader.base.exception.OrderException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.*;

import static com.estafet.kafka.trader.base.Constants.getOrderId;

/**
 * Created by Delcho Delov on 21.08.18.
 */
public class SeparatedOrderMatcher {
    private final PriceComparator priceComparator = new PriceComparator();
    private final SortedSet<Order> buyOrders = new ConcurrentSkipListSet<>(priceComparator);
    private final SortedSet<Order> sellOrders = new ConcurrentSkipListSet<>(priceComparator);
    private static Logger log = LogManager.getLogger(SeparatedOrderMatcher.class);
    private final List<Future> activeTasks = new CopyOnWriteArrayList<>();//TODO add periodic cleaner for completed
    // matches
    private final ExecutorService executorService = Executors.newWorkStealingPool();
//    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

//    private final Runnable cleaner = new Runnable() {
//        @Override
//        public void run() {
//            Iterator<Future> iterator = activeTasks.iterator();
//            log.debug("run match cleaner over "+activeTasks.size());
//            while (iterator.hasNext()) {
//                if (iterator.next().isDone()) {
//                    iterator.remove();
//                    log.debug("1 task finished");
//                }
//            }
//        }
//    };

    public SeparatedOrderMatcher() {
//        scheduler.scheduleAtFixedRate(cleaner, 0, 5, TimeUnit.SECONDS);
    }

    public void add(Order order) {
        activeTasks.add(executorService.submit(() -> match(order)));
    }

    public void awaitTermination() throws InterruptedException, TimeoutException, ExecutionException {
        log.debug(" await termination activeTasks.size = " + activeTasks.size());
        for (Future future : activeTasks) {
            if(future!=null) {
                future.get(10L, TimeUnit.SECONDS);
            }
        }
        log.debug("All tasks are finished");
//        cleaner.run();
//        if(activeTasks.isEmpty()){
//            return;
//        }
//        while(!activeTasks.isEmpty()){
//            log.debug("activeTasks.size = " + activeTasks.size());
//            Thread.sleep(activeTasks.size()*1000);
//            log.debug("waiting termination ....");
//        }
    }

    public SortedSet<Order> getBuyOrders() {
        log.debug("getBuyOrders() returns " + buyOrders.size() + " elements");
        return buyOrders;
    }

    public SortedSet<Order> getSellOrders() {
        log.debug("getSellOrders() returns " + sellOrders.size() + " elements");
        return sellOrders;
    }

    private void match(Order remaining) {
        if (remaining != null) {
            if (remaining.operation == OrderOperation.BUY) {
                matchBuy(remaining);
            } else {
                matchSell(remaining);
            }
        }
    }

    // TODO check if additional synchronization is needed
    private void match(final Order buyOrder, final Order sellOrder) /*throws OrderException*/ {
        try {
            check(buyOrder);
            check(sellOrder);
        } catch (OrderException ignored) {
            log.warn(ignored.getMessage());
            return;
        }
        buyOrders.remove(buyOrder);
        sellOrders.remove(sellOrder);
        Order remaining = null;
        if (buyOrder.quantity == sellOrder.quantity) {
            log.debug("Buy and sell quantity matches. No remaining shares left for a new order");
        } else if (buyOrder.quantity > sellOrder.quantity) {
            //remain new Buy order
            remaining = new Order(getOrderId(), buyOrder.userId, buyOrder.symbol,
                    OrderOperation.BUY, buyOrder.quantity - sellOrder.quantity, buyOrder.price, buyOrder.orderType,
                    buyOrder.from, buyOrder.validTo, buyOrder.id);
            buyOrders.add(remaining);
        } else {
            //remain new Sell order
            remaining = new Order(getOrderId(), sellOrder.userId, sellOrder.symbol,
                    OrderOperation.SELL, sellOrder.quantity - buyOrder.quantity, sellOrder.price,
                    sellOrder.orderType,
                    sellOrder.from, sellOrder.validTo, sellOrder.id);
            sellOrders.add(remaining);
        }
        int numberOfShares = Math.min(buyOrder.quantity, sellOrder.quantity);
        FinishedDeal finishedDeal = new FinishedDeal(buyOrder.id, sellOrder.id, numberOfShares, remaining.price,
                remaining.symbol);
        log.info(finishedDeal);

        match(remaining);
    }

    private void check(final Order order) throws OrderException {
        //TODO validate buyer/seller
        return;
    }

    private void matchBuy(Order buyOrder) {
        buyOrders.add(buyOrder);
        Order sellOrder = findMaxSeller(buyOrder.price);
        if (sellOrder == null) {
            log.debug("missing matching ask price for buy order " + buyOrder.id);
            return;
        }
        match(buyOrder, sellOrder);
    }

    private void matchSell(Order sellOrder) {
        sellOrders.add(sellOrder);
        Order buyOrder = findMinBuyer(sellOrder.price);
        if (buyOrder == null) {
            log.debug("missing matching bid price for sell order " + sellOrder.id);
            return;
        }
        match(buyOrder, sellOrder);

    }

    private Order findMinBuyer(BigDecimal price) {
        return buyOrders.stream()
                .filter((ord) -> ord.price.compareTo(price) >= 0)
                .min(new PriceComparator()).orElse(null);
    }


    private Order findMaxSeller(BigDecimal price) {
        return sellOrders.stream()
                .filter((ord) -> ord.price.compareTo(price) <= 0)
                .max(new PriceComparator()).orElse(null);
    }


//    public static void main(String[] args) {
//        BigDecimal sellPrice = new BigDecimal(123.4567).setScale(5, BigDecimal.ROUND_HALF_UP);
//        final String summary = String.format("User %d buys %d shares x %s USD from seller %d", 100L,
//                34, sellPrice.toString(), 8776L);
//        System.out.println("----------------------\n" + summary);
//    }
}
