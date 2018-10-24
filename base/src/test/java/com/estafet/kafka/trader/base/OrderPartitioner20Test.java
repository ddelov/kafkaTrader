package com.estafet.kafka.trader.base;

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

class OrderPartitioner20Test {
    private final OrderPartitioner20 partitioner = new OrderPartitioner20();
//    private final int NUM_PARTITIONS_20
    @Test
    void getSlot() {
        assertThat(partitioner.getSlot(20, 234.00, OrderOperation.BUY), is(0));
        assertThat(partitioner.getSlot(20, 234.01, OrderOperation.BUY), is(1));
        assertThat(partitioner.getSlot(20, 234.02, OrderOperation.BUY), is(2));
        assertThat(partitioner.getSlot(20, 234.03, OrderOperation.BUY), is(3));
        assertThat(partitioner.getSlot(20, 234.04, OrderOperation.BUY), is(4));
        assertThat(partitioner.getSlot(20, 234.05, OrderOperation.BUY), is(5));
        assertThat(partitioner.getSlot(20, 234.06, OrderOperation.BUY), is(6));
        assertThat(partitioner.getSlot(20, 234.07, OrderOperation.BUY), is(7));
        assertThat(partitioner.getSlot(20, 234.08, OrderOperation.BUY), is(8));
        assertThat(partitioner.getSlot(20, 234.09, OrderOperation.BUY), is(9));
        assertThat(partitioner.getSlot(20, 234.10, OrderOperation.BUY), is(0));

        assertThat(partitioner.getSlot(20, 234.20, OrderOperation.BUY), is(0));
        assertThat(partitioner.getSlot(20, 234.31, OrderOperation.BUY), is(1));
        assertThat(partitioner.getSlot(20, 234.42, OrderOperation.BUY), is(2));
        assertThat(partitioner.getSlot(20, 234.53, OrderOperation.BUY), is(3));
        assertThat(partitioner.getSlot(20, 234.64, OrderOperation.BUY), is(4));
        assertThat(partitioner.getSlot(20, 234.75, OrderOperation.BUY), is(5));
        assertThat(partitioner.getSlot(20, 234.86, OrderOperation.BUY), is(6));
        assertThat(partitioner.getSlot(20, 234.97, OrderOperation.BUY), is(7));
        assertThat(partitioner.getSlot(20, 234.88, OrderOperation.BUY), is(8));
        assertThat(partitioner.getSlot(20, 234.99, OrderOperation.BUY), is(9));
        assertThat(partitioner.getSlot(20, 234.60, OrderOperation.BUY), is(0));

        assertThat(partitioner.getSlot(20, 234.00, OrderOperation.SELL), is(10));
        assertThat(partitioner.getSlot(20, 234.01, OrderOperation.SELL), is(11));
        assertThat(partitioner.getSlot(20, 234.02, OrderOperation.SELL), is(12));
        assertThat(partitioner.getSlot(20, 234.03, OrderOperation.SELL), is(13));
        assertThat(partitioner.getSlot(20, 234.04, OrderOperation.SELL), is(14));
        assertThat(partitioner.getSlot(20, 234.05, OrderOperation.SELL), is(15));
        assertThat(partitioner.getSlot(20, 234.06, OrderOperation.SELL), is(16));
        assertThat(partitioner.getSlot(20, 234.07, OrderOperation.SELL), is(17));
        assertThat(partitioner.getSlot(20, 234.08, OrderOperation.SELL), is(18));
        assertThat(partitioner.getSlot(20, 234.09, OrderOperation.SELL), is(19));
        assertThat(partitioner.getSlot(20, 234.10, OrderOperation.SELL), is(10));

        assertThat(partitioner.getSlot(20, 234.20, OrderOperation.SELL), is(10));
        assertThat(partitioner.getSlot(20, 234.31, OrderOperation.SELL), is(11));
        assertThat(partitioner.getSlot(20, 234.42, OrderOperation.SELL), is(12));
        assertThat(partitioner.getSlot(20, 234.53, OrderOperation.SELL), is(13));
        assertThat(partitioner.getSlot(20, 234.64, OrderOperation.SELL), is(14));
        assertThat(partitioner.getSlot(20, 234.75, OrderOperation.SELL), is(15));
        assertThat(partitioner.getSlot(20, 234.86, OrderOperation.SELL), is(16));
        assertThat(partitioner.getSlot(20, 234.97, OrderOperation.SELL), is(17));
        assertThat(partitioner.getSlot(20, 234.88, OrderOperation.SELL), is(18));
        assertThat(partitioner.getSlot(20, 234.99, OrderOperation.SELL), is(19));
        assertThat(partitioner.getSlot(20, 234.60, OrderOperation.SELL), is(10));
    }
    @Test
    void getSlotAll(){
        for (double cents=0.01; cents<1.00; cents+=0.01 ) {
            assertThat(partitioner.getSlot(1, cents, OrderOperation.BUY), is(0));
            assertThat(partitioner.getSlot(1, cents, OrderOperation.SELL), is(0));
            assertThat(partitioner.getSlot(2, cents, OrderOperation.BUY), is(0));
            assertThat(partitioner.getSlot(2, cents, OrderOperation.SELL), is(1));
            assertThat(partitioner.getSlot(16, cents, OrderOperation.BUY), lessThan(8));
            assertThat(partitioner.getSlot(16, cents, OrderOperation.SELL), greaterThanOrEqualTo(8));
            assertThat(partitioner.getSlot(16, cents, OrderOperation.SELL), lessThan(16));
        }
    }
}