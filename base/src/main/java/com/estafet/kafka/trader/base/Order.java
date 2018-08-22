package com.estafet.kafka.trader.base;

import com.estafet.kafka.trader.base.json.OrderDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.io.Serializable;
import java.math.BigDecimal;
import java.security.InvalidParameterException;
import java.util.Calendar;
import java.util.Objects;

/**
 * Created by Delcho Delov on 01.08.18.
 * Immutable
 */
//@JsonSerialize(contentUsing= OrderSerializer.class)
@JsonDeserialize(contentUsing= OrderDeserializer.class)
public class Order implements Serializable {
    public final long id;
    public final long userId;
    public final String symbol;
    public final OrderOperation operation;
    public final int quantity;
    public final BigDecimal price;
    public final OrderType orderType;//default OrderType.Limit;
    public final Calendar from;
    public final OrderValidity orderValidity;//default OrderValidity.GTC;
    public final Calendar validTo;
    public final Long parentOrderId;

    public Order(long id, long userId, String symbol, OrderOperation operation, int quantity, BigDecimal price,
                 OrderType orderType, Calendar from, Calendar validTo, Long parentOrderId) {
        this.id = id;
        this.userId = userId;
        this.symbol = symbol;
        this.operation = operation;
        this.quantity = quantity;
        this.price = price;
        this.orderType = orderType;
        this.from = from;
        this.validTo = validTo;
        this.parentOrderId = parentOrderId;
        if(validTo==null){
            this.orderValidity = OrderValidity.GTC;
        }else{
            this.orderValidity = OrderValidity.Date;
        }
    }
    //with OrderValidity instead of validTo
    public Order(long id, long userId, String symbol, OrderOperation operation, int quantity, BigDecimal price,
                 OrderType orderType, Calendar from, OrderValidity orderValidity, Long parentOrderId) {
        this.id = id;
        this.userId = userId;
        this.symbol = symbol;
        this.operation = operation;
        this.quantity = quantity;
        this.price = price;
        this.orderType = orderType;
        this.from = from;
        this.orderValidity = orderValidity;
        Calendar validTo = Calendar.getInstance();
        switch (orderValidity){
            case Day: validTo.add(Calendar.DAY_OF_YEAR, 1);break;
            case OneWeek: validTo.add(Calendar.WEEK_OF_YEAR, 1);break;
            case GTC:validTo=null;break;
            default:throw new InvalidParameterException("като знаеш датата защо ползваш този констуктор!?!");
        }
        this.validTo = validTo;
        this.parentOrderId = parentOrderId;
    }
    //default from=NOW, validity=GTC & parentId=NULL
    public Order(long id, long userId, String symbol, OrderOperation operation, int quantity, BigDecimal price,
                 OrderType orderType) {
        this(id, userId, symbol, operation, quantity, price, orderType, Calendar.getInstance(), OrderValidity.GTC,
                null);
    }
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Order order = (Order) o;
        return id == order.id &&
                userId == order.userId &&
                quantity == order.quantity &&
                Objects.equals(symbol, order.symbol) &&
                operation == order.operation &&
                Objects.equals(price, order.price) &&
                orderType == order.orderType &&
                orderValidity == order.orderValidity &&
                Objects.equals(from, order.from) &&
                Objects.equals(validTo, order.validTo) &&
                Objects.equals(parentOrderId, order.parentOrderId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, userId, symbol, operation, quantity, price, orderType, orderValidity, from, validTo, parentOrderId);
    }

    @Override
    public String toString() {
        return "Order{" +
                "id=" + id +
                ", userId=" + userId +
                ", symbol='" + symbol + '\'' +
                ", operation=" + operation +
                ", quantity=" + quantity +
                ", price=" + price.setScale(5, BigDecimal.ROUND_HALF_UP).toString() +
                ", orderType=" + orderType +
                ", orderValidity=" + orderValidity +
                ", from=" + from +
                ", validTo=" + validTo +
                ", parentOrderId=" + parentOrderId +
                '}';
    }
}
