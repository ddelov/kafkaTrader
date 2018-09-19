package com.estafet.kafka.trader.base.comparators;

import com.estafet.kafka.trader.base.Order;

import java.util.Comparator;

/**
 * Created by Delcho Delov on 21.08.18.
 */
public class PriceComparator implements Comparator<Order> {

    @Override
    public int compare(Order order, Order t1) {
        if(order==null){
            if(t1==null){
                return 0;
            }else{
                return -1;
            }
        }else{
            if(t1==null){
                return 1;
            }
        }
        int priceC = order.price.compareTo(t1.price);
        if (priceC != 0) {
            return priceC;
        } else {
            int quontC = order.quantity - t1.quantity;
            if (quontC != 0) {
                return quontC;
            } else {
                int operC = order.operation.compareTo(t1.operation);
                if (operC != 0) {
                    return operC;
                } else {
                    return order.from.compareTo(t1.from);
                }
            }
        }
    }
}
