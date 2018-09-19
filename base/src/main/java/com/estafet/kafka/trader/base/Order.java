package com.estafet.kafka.trader.base;

import com.estafet.kafka.trader.base.json.OrderDeserializer;
import com.estafet.kafka.trader.base.json.OrderSer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.security.InvalidParameterException;
import java.util.Calendar;
import java.util.Objects;
import java.util.Properties;

import static com.estafet.kafka.trader.base.Constants.TRADER_SERVERS;
import static com.estafet.kafka.trader.base.json.OrderDeserializer.getOrder;

/**
 * Created by Delcho Delov on 01.08.18.
 * Immutable
 */
//@JsonSerialize(contentUsing= OrderSerializer.class)
@JsonDeserialize(contentUsing= OrderDeserializer.class)
public class Order implements Serializable {
    private static Logger log = Logger.getLogger(Order.class);

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

        return id == order.id;
    }

    @Override
    public int hashCode() {
        return (int) (id ^ (id >>> 32));
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

    public static String printHeader() {
        final String summary = String.format("%20s |%20s |%6s |%10s |%8s |%15s |%10s |%20s","id", "userId", "symbol",
                "operation", "quantity", "price", "orderType", "parentOrderId");
        return summary;
    }
    public String printAsTable() {
        final String summary = String.format("%20d |%20d |%6s |%10s |%8d |%15s |%10s |%20d",id, userId, symbol,
                operation, quantity, price.setScale(5, BigDecimal.ROUND_HALF_UP).toString(), orderType, parentOrderId);
        return summary;
    }

    public static void main(String[] args) throws IOException {
        String log4jConfPath = "/home/ddelov/gitRepo/kafkaTest/base/src/main/resources/log4j.properties";
        PropertyConfigurator.configure(log4jConfPath);

        String json = "{\"id\":23170537000000103,\"userId\":23170537000000503,\"symbol\":\"AAPL\"," +
                "\"operation\":\"SELL\",\"quantity\":156,\"price\":100.32508097890245,\"orderType\":\"Limit\"," +
                "\"from\":1535033137272,\"orderValidity\":\"GTC\",\"validTo\":null,\"parentOrderId\":null}";
        ObjectMapper mapper = new ObjectMapper();
        final JsonNode node = mapper.readValue(json, JsonNode.class);
        final Order order= getOrder(node);
        log.info(printHeader());
        log.info(order.printAsTable());
    }

}
