package com.estafet.kafka.trader.consumer;

import com.estafet.kafka.trader.base.FinishedDeal;
import com.estafet.kafka.trader.base.Order;
import com.estafet.kafka.trader.base.OrderOperation;
import com.estafet.kafka.trader.base.comparators.PriceComparator;
import com.estafet.kafka.trader.base.exception.OrderException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.math.BigDecimal;
import java.util.List;
import java.util.SortedSet;
import java.util.concurrent.*;
import java.util.stream.Stream;

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
        return buyOrders;
    }

    public SortedSet<Order> getSellOrders() {
        return sellOrders;
    }

    protected void match(Order order) {
        if (order != null) {
            if (order.operation == OrderOperation.BUY) {
                matchBuy(order);
            } else {
                matchSell(order);
            }
        }
    }

    // TODO check if additional synchronization is needed
    protected void match(final Order buyOrder, final Order sellOrder) /*throws OrderException*/ {
        try {
            check(buyOrder);
            check(sellOrder);
        } catch (OrderException ignored) {
            log.warn(ignored.getMessage());
            return;
        }
        getBuyOrders().remove(buyOrder);
        getSellOrders().remove(sellOrder);
        Order remaining = null;
        if (buyOrder.quantity == sellOrder.quantity) {
            log.debug("Buy and sell quantity matches. No remaining shares left for a new order");
        } else if (buyOrder.quantity > sellOrder.quantity) {
            //remain new Buy order
            remaining = new Order(getOrderId(), buyOrder.userId, buyOrder.symbol,
                    OrderOperation.BUY, buyOrder.quantity - sellOrder.quantity, buyOrder.price, buyOrder.orderType,
                    buyOrder.from, buyOrder.validTo, buyOrder.id);
            getBuyOrders().add(remaining);
        } else {
            //remain new Sell order
            remaining = new Order(getOrderId(), sellOrder.userId, sellOrder.symbol,
                    OrderOperation.SELL, sellOrder.quantity - buyOrder.quantity, sellOrder.price,
                    sellOrder.orderType,
                    sellOrder.from, sellOrder.validTo, sellOrder.id);
            getSellOrders().add(remaining);
        }
        int numberOfShares = Math.min(buyOrder.quantity, sellOrder.quantity);
        FinishedDeal finishedDeal = new FinishedDeal(buyOrder.id, sellOrder.id, numberOfShares, remaining.price,
                remaining.symbol);
        log.info(finishedDeal);

        match(remaining);
    }

    private void check(final Order order) throws OrderException {
        //TODO validate buyer has enough available money to cover the shares costs
        //TODO validate seller owns enough shares for the transaction
        return;
    }

    protected void matchBuy(Order buyOrder) {
        if(buyOrder==null){
            return;
        }
        getBuyOrders().add(buyOrder);
        Order sellOrder = findMaxSeller(buyOrder.price);
        if (sellOrder == null) {
            log.debug("missing matching ask price for buy order " + buyOrder.id);
            return;
        }
        match(buyOrder, sellOrder);
    }

    protected void matchSell(Order sellOrder) {
        if(sellOrder==null){
            return;
        }
        getSellOrders().add(sellOrder);
        Order buyOrder = findMinBuyer(sellOrder.price);
        if (buyOrder == null) {
            log.debug("missing matching bid price for sell order " + sellOrder.id);
            return;
        }
        match(buyOrder, sellOrder);

    }

    protected Order findMinBuyer(BigDecimal price) {
        if(price==null){
            return null;
        }
        final Stream<Order> stream = getBuyOrders().stream().filter((ord) -> ord.price.compareTo(price) >= 0);
//        final PriceComparator priceComparator = new PriceComparator();
//        stream.sorted(priceComparator).forEach(System.out::println);
        final Order order = stream.min(priceComparator).orElse(null);
        return order;
    }


    protected Order findMaxSeller(BigDecimal price) {
        if(price==null){
            return null;
        }
        return getSellOrders().stream()
                .filter((ord) -> ord.price.compareTo(price) <= 0)
                .max(priceComparator).orElse(null);
    }


//    public static void main(String[] args) {
//        BigDecimal sellPrice = new BigDecimal(123.4567).setScale(5, BigDecimal.ROUND_HALF_UP);
//        final String summary = String.format("User %d buys %d shares x %s USD from seller %d", 100L,
//                34, sellPrice.toString(), 8776L);
//        System.out.println("----------------------\n" + summary);
//    }
}
