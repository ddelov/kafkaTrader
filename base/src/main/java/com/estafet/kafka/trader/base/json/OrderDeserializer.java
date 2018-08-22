package com.estafet.kafka.trader.base.json;

import com.estafet.kafka.trader.base.Order;
import com.estafet.kafka.trader.base.OrderOperation;
import com.estafet.kafka.trader.base.OrderType;
import com.estafet.kafka.trader.base.OrderValidity;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Calendar;

/**
 * Created by Delcho Delov on 02.08.18.
 */
public class OrderDeserializer extends JsonDeserializer<Order> {
    @Override
    public Order deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
        JsonNode node = jsonParser.getCodec().readTree(jsonParser);

        return getOrder(node);
    }

    public static Order getOrder(JsonNode node) {
        if(node==null) {
            throw new UnsupportedOperationException();
        }
        final long id = node.get("id").asLong();
        final long userId = node.get("userId").asLong();
        final String symbol = node.get("symbol").asText();
        final OrderOperation operation = OrderOperation.valueOf(node.get("operation").asText());
        final int quantity = node.get("quantity").asInt();
        final BigDecimal price = new BigDecimal(node.get("price").asText());
        final OrderType orderType = OrderType.valueOf(node.get("orderType").asText());
        final Calendar from = Calendar.getInstance();
        from.setTimeInMillis(node.get("from").asLong());
        Long parentOrderId = null;
        if(!node.get("parentOrderId").isNull()) {
            parentOrderId = node.get("parentOrderId").asLong();
        }

        final OrderValidity orderValidity = OrderValidity.valueOf(node.get("orderValidity").asText());
        Long validToRead = null;
        if(!node.get("validTo").isNull()) {
            validToRead = node.get("validTo").asLong();
            final Calendar validTo= Calendar.getInstance(); validTo.setTimeInMillis(node.get("validTo").asLong());
            return new Order(id, userId, symbol, operation, quantity, price, orderType, from, validTo, parentOrderId);
        }else{
            return new Order(id, userId, symbol, operation, quantity, price, orderType, from, orderValidity,
                    parentOrderId);
        }
    }

    public static void main(String[] args) throws IOException {
        String json = "{\"id\":100,\"userId\":4000,\"symbol\":\"ITL\",\"operation\":\"BUY\",\"quantity\":157," +
                "\"price\":100.33689215313451,\"orderType\":\"Limit\",\"from\":1534764402417," +
                "\"orderValidity\":\"GTC\",\"validTo\":null,\"parentOrderId\":null}";
        ObjectMapper mapper = new ObjectMapper();
        JsonNode node = mapper.readValue(json, JsonNode.class);

        Order order= getOrder(node);
        System.out.println("order = " + order);
    }
}
