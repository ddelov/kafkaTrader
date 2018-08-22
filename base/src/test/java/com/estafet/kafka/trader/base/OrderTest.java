package com.estafet.kafka.trader.base;

import com.estafet.kafka.trader.base.json.OrderDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Calendar;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Created by Delcho Delov on 02.08.18.
 */
class OrderTest {
    final Calendar from = Calendar.getInstance();
    final Calendar validTo = Calendar.getInstance();
    {
        from.set(Calendar.YEAR, 2018);
        from.set(Calendar.MONTH, 7);
        from.set(Calendar.DAY_OF_MONTH, 2);
        from.set(Calendar.HOUR_OF_DAY, 0);
        from.set(Calendar.MINUTE, 0);
        from.set(Calendar.SECOND, 0);
        from.set(Calendar.MILLISECOND, 0);

        validTo.set(Calendar.HOUR_OF_DAY, 0);
        validTo.set(Calendar.MINUTE, 0);
        validTo.set(Calendar.SECOND, 0);
        validTo.set(Calendar.MILLISECOND, 0);
        validTo.set(Calendar.YEAR, 2018);
        validTo.set(Calendar.MONTH, 7);
        validTo.set(Calendar.DAY_OF_MONTH, 20);
    }
    final Order order = new Order(100L, 308977345L, "AAPL", OrderOperation.SELL, 100,
            BigDecimal.valueOf(1743.3256), OrderType.Limit, from, OrderValidity.GTC, null);
    final String jsonValue = "{\"id\":100,\"userId\":308977345,\"symbol\":\"AAPL\",\"operation\":\"SELL\"," +
            "\"quantity\":100,\"price\":1743.3256,\"orderType\":\"Limit\",\"from\":1533157200000,\"orderValidity\":\"GTC\",\"validTo\":null,\"parentOrderId\":null}";

    @Test
    public void deserialiser() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        SimpleModule module = new SimpleModule("OrderDeserializer");
        module.addDeserializer(Order.class, new OrderDeserializer());
        mapper.registerModule(module);

        final Order readValue = mapper.readValue(jsonValue, Order.class);
        assertThat(readValue.id, is(order.id));
        assertThat(readValue.userId, is(order.userId));
        assertThat(readValue.symbol, is(order.symbol));
        assertThat(readValue.operation, is(order.operation));
        assertThat(readValue.quantity, is(order.quantity));
        assertThat(readValue.price, is(order.price));
        assertThat(readValue.orderType, is(order.orderType));
        assertThat(readValue.orderValidity, is(order.orderValidity));
        assertThat(readValue.from, is(order.from));
        assertThat(readValue.validTo, is(order.validTo));
        assertThat(readValue.parentOrderId, is(order.parentOrderId));
    }
    @Test
    public void serialiser() {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            final String value = objectMapper.writeValueAsString(order);
            assertThat(value, is(jsonValue));
        } catch (IOException e) {
            fail();
        }
    }

}