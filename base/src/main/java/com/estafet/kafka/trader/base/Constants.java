package com.estafet.kafka.trader.base;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by Delcho Delov on 03.08.18.
 */
public final class Constants {
    public final static String[] SHARES = {"AAPL", "GOOG", "GM", "AMZN", "FB", "MSFT", "ITL"};
    public final static String TRADER_SERVERS = "localhost:9092,localhost:9093,localhost:9094";
    public final static String TOPIC_INCOMING_ORDERS = "incoming-orders";
    public final static List<String> SHARES_LIST = new ArrayList<>(Arrays.asList(SHARES));
}
