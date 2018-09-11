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
import static com.estafet.kafka.trader.base.Constants.getOrderId;

/**
 * Created by Delcho Delov on 21.08.18.
 */
public class UncheckedOrderMatcher{
    private final SortedSet<Order> buyOrders;
    private final SortedSet<Order> sellOrders;
    private static Logger log = LogManager.getLogger(UncheckedOrderMatcher.class);

    public UncheckedOrderMatcher(SortedSet<Order> buyOrders, SortedSet<Order> sellOrders) {
        this.buyOrders = buyOrders;
        this.sellOrders = sellOrders;
    }

    void matchBuy(Order buyOrder) throws OrderException {
        buyOrders.add(buyOrder);
        Order sellOrder = findMaxSeller(buyOrder.price);
        if (sellOrder == null) {
            throw new OrderException("missing matching sell order");
        }
        match(buyOrder, sellOrder);
    }
    void matchSell(Order sellOrder) {
        sellOrders.add(sellOrder);
        Order buyOrder = findMinBuyer(sellOrder.price);
        if (buyOrder == null) {
            throw new OrderException("missing matching buy order");
        }
        match(buyOrder, sellOrder);

    }


    private Order findMaxSeller(BigDecimal price) {
        return sellOrders.stream()
                .filter((ord) -> ord.price.compareTo(price) <= 0)
                .max(new PriceComparator()).orElse(null);
    }

    // TODO check if additional synchronization is needed
    public void match(final Order buyOrder, final Order sellOrder) throws OrderException {
            check(buyOrder);
            check(sellOrder);
            buyOrders.remove(buyOrder);
            sellOrders.remove(sellOrder);
            Order remaining=null;
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

            if (remaining != null){
             if(remaining.operation == OrderOperation.BUY) {
                 matchBuy(remaining);
             }else {
                 matchSell(remaining);
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
