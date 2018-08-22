package com.estafet.kafka.trader.base.comparators;

import com.estafet.kafka.trader.base.Order;

import java.util.Comparator;

/**
 * Created by Delcho Delov on 21.08.18.
 */
public class PriceComparator implements Comparator<Order> {

    @Override
    public int compare(Order order, Order t1) {
        return order.price.compareTo(t1.price);
    }
}
