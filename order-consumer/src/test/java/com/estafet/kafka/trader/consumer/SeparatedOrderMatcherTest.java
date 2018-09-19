package com.estafet.kafka.trader.consumer;

import com.estafet.kafka.trader.base.Order;
import com.estafet.kafka.trader.base.OrderOperation;
import com.estafet.kafka.trader.base.OrderType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.math.BigDecimal;
import java.util.SortedSet;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

/**
 * Created by Delcho Delov on 17.09.18.
 */
@RunWith(MockitoJUnitRunner.class)
public class SeparatedOrderMatcherTest {

    private final Order bOrderZero = new Order(100L, 200L, "AAPL", OrderOperation.BUY, 100, BigDecimal.ZERO,
                                            OrderType.Limit);
    private final Order bOrderOne = new Order(101L, 201L, "AAPL", OrderOperation.BUY, 15, BigDecimal.ONE,
            OrderType.Limit);
    private final Order bOrderTen = new Order(110L, 210L, "AAPL", OrderOperation.BUY, 16, BigDecimal.TEN,
            OrderType.Limit);
    private final Order sOrderOne = new Order(201L, 101L, "AAPL", OrderOperation.SELL, 26, BigDecimal.ONE,
            OrderType.Limit);
    private final Order sOrderTen = new Order(210L, 110L, "AAPL", OrderOperation.SELL, 3, BigDecimal.TEN,
            OrderType.Limit);
    @Mock
    private SeparatedOrderMatcher mockMatcher;
    @Mock
    private SortedSet<Order> mockOrders;


    private SeparatedOrderMatcher matcher = new SeparatedOrderMatcher();

    @Before
    public void setUp() {
        matcher.getBuyOrders().clear();
        matcher.getSellOrders().clear();
    }

    @Test
    public void findMinBuyerNoPrice(){
        assertNull(matcher.findMinBuyer(new BigDecimal(23.56)));

        matcher.getBuyOrders().add(bOrderOne);
        matcher.getBuyOrders().add(bOrderTen);

        assertNull(matcher.findMinBuyer(null));
        assertThat(matcher.findMinBuyer(new BigDecimal(0.56)), is(bOrderOne));
        assertThat(matcher.findMinBuyer(BigDecimal.ONE), is(bOrderOne));
        assertThat(matcher.findMinBuyer(new BigDecimal(1.444)), is(bOrderTen));
        assertNull(matcher.findMinBuyer(new BigDecimal(451.444)));
    }
    @Test
    public void findMaxSell(){
        assertNull(matcher.findMaxSeller(BigDecimal.ONE));

        matcher.getSellOrders().add(sOrderOne);
        matcher.getSellOrders().add(sOrderTen);
        assertNull(matcher.findMaxSeller(null));
        assertNull(matcher.findMaxSeller(new BigDecimal(0.56)));
        assertThat(matcher.findMaxSeller(BigDecimal.ONE), is(sOrderOne));
        assertThat(matcher.findMaxSeller(new BigDecimal(1.444)), is(sOrderOne));
        assertThat(matcher.findMaxSeller(new BigDecimal(8.2345)), is(sOrderOne));
        assertThat(matcher.findMaxSeller(new BigDecimal(166.444)), is(sOrderTen));
    }

    @Test
    public void matchOne2One(){
//        matcher.getBuyOrders().add(bOrderOne);
        matcher.getBuyOrders().add(bOrderTen);
//        matcher.getSellOrders().add(sOrderOne);
        matcher.getSellOrders().add(sOrderTen);

        assertThat(matcher.getBuyOrders().size(), is(1));
//        assertThat(matcher.getBuyOrders().contains(bOrderOne), is(true));
        assertThat(matcher.getBuyOrders().contains(bOrderTen), is(true));
        assertThat(matcher.getSellOrders().size(), is(1));
        assertThat(matcher.getSellOrders().contains(sOrderTen), is(true));
//        assertThat(matcher.getSellOrders().contains(sOrderOne), is(true));

        matcher.match(bOrderTen, sOrderTen);
        assertThat(matcher.getBuyOrders().size(), is(1));
//        assertThat(matcher.getBuyOrders().contains(bOrderOne), is(true));
//        assertThat(matcher.getBuyOrders().contains(bOrderTen), is(false));
//        matcher.getBuyOrders().remove(bOrderOne);
        Order remaining = matcher.getBuyOrders().first();
        assertThat(remaining.price, is(sOrderTen.price));
        assertThat(remaining.parentOrderId, is(bOrderTen.id));
        assertThat(remaining.quantity, is(13));
        assertThat(remaining.orderType, is(OrderType.Limit));
        assertThat(remaining.operation, is(OrderOperation.BUY));
        assertThat(remaining.userId, is(bOrderTen.userId));
        assertThat(remaining.from, is(bOrderTen.from));
        assertThat(remaining.symbol, is(bOrderTen.symbol));
        assertThat(remaining.orderValidity, is(bOrderTen.orderValidity));

        assertThat(matcher.getSellOrders().size(), is(0));
//        assertThat(matcher.getSellOrders().contains(sOrderTen), is(false));
//        assertThat(matcher.getSellOrders().contains(sOrderOne), is(true));

    }

}