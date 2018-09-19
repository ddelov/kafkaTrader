package com.estafet.kafka.trader.base.comparators;

import com.estafet.kafka.trader.base.Order;
import com.estafet.kafka.trader.base.OrderOperation;
import com.estafet.kafka.trader.base.OrderType;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Calendar;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.core.Is.is;


class PriceComparatorTest {
    private final PriceComparator priceComparator = new PriceComparator();

    private final Order bOrderZero = new Order(100L, 200L, "AAPL", OrderOperation.BUY, 100, BigDecimal.ZERO,
            OrderType.Limit);
    private final Order bOrderOne = new Order(101L, 201L, "AAPL", OrderOperation.BUY, 15, BigDecimal.ONE,
            OrderType.Limit);
    private final Order bOrderTen = new Order(110L, 210L, "AAPL", OrderOperation.BUY, 16, BigDecimal.TEN,
            OrderType.Limit);
    private final Order sOrderOne = new Order(201L, 101L, "AAPL", OrderOperation.SELL, 26, BigDecimal.ONE,
            OrderType.Limit);
    private final Order sOrderTen3 = new Order(210L, 110L, "AAPL", OrderOperation.SELL, 3, BigDecimal.TEN,
            OrderType.Limit);
    private final Order bOrderTen3 = new Order(210L, 110L, "AAPL", OrderOperation.BUY, 3, BigDecimal.TEN,
            OrderType.Limit);

    @Test
    void compare() {
        assertThat(priceComparator.compare(null, null), is(0));
        assertThat(priceComparator.compare(bOrderOne, null), is(greaterThan(0)));
        assertThat(priceComparator.compare(null, sOrderOne), is(lessThan(0)));

        assertThat(priceComparator.compare(bOrderZero, bOrderZero), is(0));//price
        assertThat(priceComparator.compare(bOrderZero, bOrderOne), is(lessThan(0)));//price
        assertThat(priceComparator.compare(bOrderZero, bOrderTen), is(lessThan(0)));//price
        assertThat(priceComparator.compare(bOrderOne, sOrderOne), is(lessThan(0)));//quantity

        assertThat(priceComparator.compare(bOrderTen3, sOrderTen3), is(lessThan(0)));

        final Calendar past = Calendar.getInstance();
        past.add(Calendar.MINUTE, -54);
        final Calendar fut = Calendar.getInstance();
        fut.add(Calendar.MINUTE, 27);
        final Order bOrderZeroPast = new Order(100L, 200L, "AAPL", OrderOperation.BUY, 100, BigDecimal.ZERO,
                OrderType.Limit, past, fut, null);
        assertThat(priceComparator.compare(bOrderZeroPast, bOrderZero), is(lessThan(0)));
    }
}