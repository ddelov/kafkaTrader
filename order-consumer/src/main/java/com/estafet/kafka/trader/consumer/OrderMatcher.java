package com.estafet.kafka.trader.consumer;

import com.estafet.kafka.trader.base.FinishedDeal;
import com.estafet.kafka.trader.base.Order;
import com.estafet.kafka.trader.base.OrderOperation;
import com.estafet.kafka.trader.base.comparators.PriceComparator;
import com.estafet.kafka.trader.base.exception.OrderException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.math.BigDecimal;
import java.util.SortedSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.estafet.kafka.trader.base.Constants.PRICE_MAX_VALUE;

/**
 * Created by Delcho Delov on 21.08.18.
 */
public class OrderMatcher {
    private final SortedSet<Order> orders;
    private static Logger log = LogManager.getLogger(OrderMatcher.class);

    public OrderMatcher(SortedSet<Order> orders) {
        this.orders = orders;
    }

    void match(Order order) throws OrderException {
            if (order == null) {
                throw new NullPointerException("missing order");
            }
            if(order.operation!=OrderOperation.BUY && order.operation!=OrderOperation.SELL){
                throw new OrderException("Wrong order operation - only BUY and SELL could be matched");
            }
            orders.add(order);
            Order buyOrder = null;
            Order sellOrder = null;
            if(order.operation==OrderOperation.BUY){
                buyOrder = order;
                sellOrder = findMaxSeller(buyOrder.price);
                if (sellOrder == null) {
                    throw new OrderException("missing matching sell order");
                }
            }else{
                sellOrder = order;
                buyOrder = findMinBuyer(sellOrder.price);
                if (buyOrder == null) {
                    throw new OrderException("missing matching buy order");
                }
            }
            match(buyOrder, sellOrder);
    }

    private Order findMinBuyer(BigDecimal price) {
        return orders.stream().filter((ord) -> ord.operation.equals(OrderOperation.BUY))
                .filter((ord) -> ord.price.compareTo(price) >= 0)
                .min(new PriceComparator()).orElse(null);
    }

    private Order findMaxSeller(BigDecimal price) {
        return orders.stream().filter((ord) -> ord.operation.equals(OrderOperation.SELL))
                .filter((ord) -> ord.price.compareTo(price) <= 0)
                .max(new PriceComparator()).orElse(null);
    }

    private Iterable<Order> getBuyers() {
        final Order minSeller = findMinSeller();
        final BigDecimal minPrice = minSeller == null ? PRICE_MAX_VALUE : minSeller.price;
        return orders.stream().filter((ord) -> ord.operation.equals(OrderOperation.BUY))
                .filter((ord) -> ord.price.compareTo(minPrice) >= 0) //Buy orders that have a chance for matching
                .sorted(new PriceComparator())
                .collect(Collectors.toList());
    }

    private Stream<Order> getSellOrders() {
        return orders.stream()
                .filter((ord) -> ord.operation.equals(OrderOperation.SELL));
    }

    private Order findMinSeller() {
        return getSellOrders().min(new PriceComparator()).orElse(null);
    }

    public void match(final Order buyOrder, final Order sellOrder) throws OrderException {
        if (buyOrder != null && sellOrder != null && buyOrder.price.compareTo(sellOrder.price) >= 0) {
            check(buyOrder);
            check(sellOrder);
            Order remaining = null;
            if (buyOrder.quantity == sellOrder.quantity) {
                log.debug("Buy and sell quantity matches. No remaining shares left for a new order");
            } else if (buyOrder.quantity > sellOrder.quantity) {
                //remain new Buy order
                //remain new Buy order
                remaining = new Order(buyOrder.id + 1000000L, buyOrder.userId, buyOrder.symbol,
                        OrderOperation.BUY, buyOrder.quantity - sellOrder.quantity, buyOrder.price, buyOrder.orderType,
                        buyOrder.from, buyOrder.validTo, buyOrder.id);
            } else {
                //remain new Sell order
                remaining = new Order(sellOrder.id + 1000000L, sellOrder.userId, sellOrder.symbol,
                        OrderOperation.SELL, sellOrder.quantity - buyOrder.quantity, sellOrder.price,
                        sellOrder.orderType,
                        sellOrder.from, sellOrder.validTo, sellOrder.id);
            }
            synchronized (this) {
                orders.remove(buyOrder);
                orders.remove(sellOrder);
                if (remaining != null) {
                    orders.add(remaining);
                }
            }
            int numberOfShares = Math.min(buyOrder.quantity, sellOrder.quantity);
            FinishedDeal finishedDeal = new FinishedDeal(buyOrder.id, sellOrder.id, numberOfShares, remaining.price,
                    remaining.symbol);
            log.info(finishedDeal);

            if (remaining != null && remaining.operation == OrderOperation.BUY) {
                match(remaining);
            }
        }
    }

    private void check(final Order order) throws OrderException {
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
