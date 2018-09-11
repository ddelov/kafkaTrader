package com.estafet.kafka.trader.base;

import com.estafet.kafka.trader.base.json.OrderSer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by Delcho Delov on 03.08.18.
 */
public final class Constants {
    public static final String[] SHARES = {"AAPL", "GOOG", "GM", "AMZN", "FB", "MSFT", "ITL"};
    public static final String TRADER_SERVERS = "localhost:9092,localhost:9093,localhost:9094";
    public static final String TOPIC_INCOMING_ORDERS = "incoming-orders";
    public static final List<String> SHARES_LIST = new ArrayList<>(Arrays.asList(SHARES));
    public static final BigDecimal PRICE_MAX_VALUE = BigDecimal.valueOf(Long.MAX_VALUE);
    public static final long POLL_EVERY_X_SECONDS = 17l;

    private static final AtomicLong orderSeq = new AtomicLong();
    private static final AtomicLong userSeq = new AtomicLong();
    private static final AtomicLong dealSeq = new AtomicLong();

    public static long getOrderId() {
        long datePrefix = getDatePrefix();
        return datePrefix + orderSeq.getAndIncrement();
    }

    public static long getUserId() {
        long datePrefix = getDatePrefix();
        return datePrefix + userSeq.getAndIncrement();
    }

    public static long getFinishedDealId() {
        long datePrefix = getDatePrefix();
        return datePrefix + dealSeq.getAndIncrement();
    }

    private static long getDatePrefix() {
        final SimpleDateFormat df = new SimpleDateFormat("ddHHmmss");
        final Date now = new Date();
        return new Long(df.format(now)) * 1_000_000_000;
    }

    public static Properties getProducerProperties(String clientId) {
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, TRADER_SERVERS);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 4);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 100);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, OrderSer.class);//"org.apache.kafka.common.serialization
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.estafet.kafka.trader.base.TradeOperationPartitioner");

        return props;
    }

    public static ProducerRecord<String, Order> getProducerRecord(Order order) {
        return new ProducerRecord<>(getTopic(order), /*getPartition(order), getKey(order),*/ order);
    }

    public static ProducerRecord<String, Order> createSortedProducerRecord(Order order) {
        return new ProducerRecord<>(getSortedTopic(order), /*getPartition(order), getKey(order),*/ order);
    }

    public static String getSortedTopic(Order order) {
        return "sorted" + TOPIC_INCOMING_ORDERS + order.symbol;
    }

    public static String getTopic(Order order) {
        return TOPIC_INCOMING_ORDERS + order.symbol;
    }

    public static int getPartition(Order order) {
        return order.operation.ordinal();
    }

    public static String getKey(Order order) {
        return "" + order.id;
    }

    public static String displayElapsedTime(long elapsedTime, String jobName) {
        long timeH = TimeUnit.HOURS.convert(elapsedTime, TimeUnit.NANOSECONDS);
        long remainingNanos = elapsedTime - TimeUnit.NANOSECONDS.convert(timeH, TimeUnit.HOURS);//1h = 60 min
        long timeMinutes = TimeUnit.MINUTES.convert(remainingNanos, TimeUnit.NANOSECONDS);//1min = 60 sec
        remainingNanos = remainingNanos - TimeUnit.NANOSECONDS.convert(timeMinutes, TimeUnit.MINUTES);
        long timeInSec = TimeUnit.SECONDS.convert(remainingNanos, TimeUnit.NANOSECONDS);//1sec = 1000 * millis
        remainingNanos = remainingNanos - TimeUnit.NANOSECONDS.convert(timeInSec, TimeUnit.SECONDS);
        long timeMil = TimeUnit.MILLISECONDS.convert(remainingNanos, TimeUnit.NANOSECONDS);//1 mil=1,000,000 nanos

        final StringBuilder sb = new StringBuilder(jobName!=null?jobName:"Job");
        sb.append(" finished in ");
        if (timeH > 0) {
            sb.append(timeH).append(timeH != 1 ? " hours" : " hour");
        }
        if (timeMinutes > 0) {
            if (timeH > 0) {
                sb.append(timeInSec > 0 ? ", " : " and ");
            }
            sb.append(timeMinutes).append(timeMinutes != 1 ? " minutes" : " minute");
        }
        if (timeInSec > 0) {
            if (timeH > 0 || timeMinutes > 0) {
                sb.append(" and ");
            }
            sb.append(timeInSec).append(timeInSec != 1 ? " seconds" : " second");
        }
        if (timeH > 0 || timeMinutes > 0 || timeInSec > 0) {
            sb.append(" and ");
        }
        sb.append(timeMil).append(timeMil != 1 ? " milliseconds" : " millisecond");

        return sb.toString();
    }

    public static void main(String[] args) {
        System.out.println("getOrderId() = " + getOrderId());
        System.out.println("getUserId() = " + getUserId());
        System.out.println("getOrderId() = " + getOrderId());
        System.out.println("getOrderId() = " + getOrderId());
        System.out.println("getOrderId() = " + getOrderId());
        System.out.println("getOrderId() = " + getOrderId());
        System.out.println("getFinishedDealId() = " + getFinishedDealId());
    }
}
