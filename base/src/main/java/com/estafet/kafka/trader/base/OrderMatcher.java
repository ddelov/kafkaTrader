package com.estafet.kafka.trader.base;

import com.estafet.kafka.trader.base.comparators.PriceComparator;
import org.apache.log4j.Logger;

import java.util.Optional;
import java.util.SortedSet;
import java.util.stream.Stream;

/**
 * Created by Delcho Delov on 21.08.18.
 */
public class OrderMatcher {
    private final SortedSet<Order> orders;
    private static Logger log = Logger.getLogger(OrderMatcher.class);

    public OrderMatcher(SortedSet<Order> orders) {
        this.orders = orders;
    }

    public int proceed(){
        log.debug("in proceed");
        int matches = 0;
        boolean matchFound = true;
        do {
            Order maxBuyer = findMaxBuyer();
            Order minSeller = findMinSeller();
            if(maxBuyer==null || minSeller==null){
                log.warn("buyer or seller missing");
                matchFound = false;
            }else {
                log.debug("minSeller = " + minSeller);
                log.debug("maxBuyer = " + maxBuyer);
                try {
                    matchFound = match(maxBuyer, minSeller);
                    log.info("matchFound = "+matchFound);
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            }
            if(matchFound){
                matches++;
            }
        }while(matchFound);
        return matches;
    }
    private Stream<Order> getBuyOrders(){
        return orders.stream()
                .filter((ord) -> ord.operation.equals(OrderOperation.BUY));
    }
    private Stream<Order> getSellOrders(){
        return orders.stream()
                .filter((ord) -> ord.operation.equals(OrderOperation.SELL));
    }
    private Order findMaxBuyer() {
        final Optional<Order> max = getBuyOrders().max(new PriceComparator());
        return max.isPresent()?max.get():null;
    }

    private Order findMinSeller() {
        final Optional<Order> min = getSellOrders().min(new PriceComparator());
        return min.isPresent()?min.get():null;
    }

    public boolean match(final Order buyOrder, final Order sellOrder) {
        if (buyOrder != null && sellOrder != null && buyOrder.price.compareTo(sellOrder.price) >= 0) {
            check(buyOrder);
            check(sellOrder);
            orders.remove(buyOrder);
            orders.remove(sellOrder);
            if(buyOrder.quantity == sellOrder.quantity){
                log.debug("suy and sell quantity matches");
            }else if(buyOrder.quantity > sellOrder.quantity){
                //remain new Buy order
                final Order order = new Order(buyOrder.id + 1000000L, buyOrder.userId, buyOrder.symbol,
                        OrderOperation.BUY, buyOrder.quantity - sellOrder.quantity, buyOrder.price, buyOrder.orderType,
                        buyOrder.from, buyOrder.validTo, buyOrder.id);
                orders.add(order);
            }else{
                //remain new Sell order
                final Order order = new Order(sellOrder.id + 1000000L, sellOrder.userId, sellOrder.symbol,
                        OrderOperation.SELL, sellOrder.quantity - buyOrder.quantity, sellOrder.price,
                        sellOrder.orderType,
                        sellOrder.from, sellOrder.validTo, sellOrder.id);
                orders.add(order);
            }
            int numberOfShares = Math.min (buyOrder.quantity, sellOrder.quantity);
            final String summary = String.format("User %d buys %d %s shares x %s USD from seller %d", buyOrder.userId,
                    numberOfShares, sellOrder.symbol, sellOrder.price.toString(), sellOrder.userId);
            log.info("----------------------\n" + summary);
            return true;
        }
        return false;
    }

    private void check(final Order order){
        //TODO validate buyer/seller
        return;
    }

//    public static void main(String[] args) {
//        BigDecimal sellPrice = new BigDecimal(123.4567).setScale(5, BigDecimal.ROUND_HALF_UP);
//        final String summary = String.format("User %d buys %d shares x %s USD from seller %d", 100L,
//                34, sellPrice.toString(), 8776L);
//        System.out.println("----------------------\n" + summary);
//    }
}
