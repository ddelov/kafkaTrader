package com.estafet.kafka.trader.base.json;

import com.estafet.kafka.trader.base.Order;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.util.Map;

public class OrderSer implements org.apache.kafka.common.serialization.Serializer<Order>, Deserializer<Order>,
        Serde<Order> {

    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public Order deserialize(String s, byte[] bytes) {
        ObjectMapper objectMapper = new ObjectMapper();
        if (bytes == null)
            return null;
        ObjectMapper mapper = new ObjectMapper();
        JsonNode node = null;
        try {
            node = mapper.readValue(new String(bytes), JsonNode.class);
            return OrderDeserializer.getOrder(node);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    @Override
    public byte[] serialize(String s, Order order) {
        byte[] serializedBytes = null;
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            serializedBytes = objectMapper.writeValueAsString(order).getBytes("UTF-8");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return serializedBytes;
    }

    @Override
    public void close() {

    }

    @Override
    public Serializer<Order> serializer() {
        return this;
    }

    @Override
    public Deserializer<Order> deserializer() {
        return this;
    }
}
