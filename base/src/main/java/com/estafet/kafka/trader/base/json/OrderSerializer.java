package com.estafet.kafka.trader.base.json;

import com.estafet.kafka.trader.base.Order;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;
import java.math.BigDecimal;

/**
 * Created by Delcho Delov on 01.08.18.
 */
public class OrderSerializer extends JsonSerializer<Order> {
    public void serialize(Order order, JsonGenerator jgen, SerializerProvider serializerProvider) throws IOException {
        jgen.writeStartObject();
        jgen.writeNumberField("id", order.id);
        jgen.writeNumberField("userId", order.userId);
        jgen.writeStringField("symbol", order.symbol);
        jgen.writeStringField("operation", order.operation.name());
        jgen.writeNumberField("quantity", order.quantity);
        jgen.writeStringField("price", order.price.setScale(5, BigDecimal.ROUND_HALF_UP).toString());
        jgen.writeStringField("orderType", order.orderType.name());
        jgen.writeStringField("orderValidity", order.orderValidity.name());
        jgen.writeNumberField("from", order.from.getTimeInMillis());
        jgen.writeNumberField("validTo", order.validTo.getTimeInMillis());
        if(order.parentOrderId != null) {
            jgen.writeNumberField("parentOrderId", order.parentOrderId);
        }
        jgen.writeEndObject();
    }
}
