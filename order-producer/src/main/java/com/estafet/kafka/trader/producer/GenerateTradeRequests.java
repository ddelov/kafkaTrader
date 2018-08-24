package com.estafet.kafka.trader.producer;

import com.estafet.kafka.trader.base.Order;
import com.estafet.kafka.trader.base.OrderOperation;
import com.estafet.kafka.trader.base.OrderType;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.estafet.kafka.trader.base.Constants.*;

/**
 * Created by Delcho Delov on 01.08.18.
 */
public class GenerateTradeRequests {

    private static Logger log = Logger.getLogger(GenerateTradeRequests.class);

    public static void main(String[] args) {
        if (args == null || args.length < 2) {
            System.out.println("please provide parameters : <generationType> <clientId>");
            System.out.println("\tgenerationType can be 'fixed' or 'timer'");
            System.out.println("\tclientId is a string identifying order producer for logging/metrics/quota");
            System.exit(0);
        }
        String type = null;
        String clientId = "OrderProducer";
        if (args != null && args.length > 1) {
            type = args[0].equalsIgnoreCase("timer") ? "timer" : "fixed";
            clientId = args[1];
        }
        String log4jConfPath = "/home/ddelov/gitRepo/kafkaTest/order-producer/src/main/resources/log4j.properties";
        PropertyConfigurator.configure(log4jConfPath);
        log.debug("producer started");

        final SimpleDateFormat df = new SimpleDateFormat("ddHHmmss");
        final Date now = new Date();
        long initialReqId = new Long(df.format(now)) * 1_000_000_000 + 100;
        long initialUserId = new Long(df.format(now)) * 1_000_000_000 + 500;
        final AtomicLong orderSeq = new AtomicLong(initialReqId);
        final AtomicLong userSeq = new AtomicLong(initialUserId);
        final Random rnd = new Random(System.nanoTime());
        final String symbol = SHARES[/*rnd.nextInt(SHARES.length)*/0];
        final GenerateTradeRequests generateTradeRequests = new GenerateTradeRequests();

        final String finalClientId = clientId;

        if (type.equals("timer")) {
            final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                executorService.shutdown();
                log.info("Shutdown executorPool");
            }));
            Runnable task = () -> {
                final Order order = generateTradeRequests.generateOrder(symbol, orderSeq, userSeq);
                try (Producer<String, Order> producer = new KafkaProducer<>(getProducerProperties(finalClientId));) {
                    producer.send(new ProducerRecord<String, Order>(TOPIC_INCOMING_ORDERS + symbol, order));
                    producer.flush();
                }
            };
            executorService.scheduleAtFixedRate(task, 0, 3, TimeUnit.SECONDS);

        } else {
            final Set<Order> orders = generateTradeRequests.generateFixedNumberOrders(symbol, 10, orderSeq,
                    userSeq);
            try (Producer<String, Order> producer = new KafkaProducer<>(getProducerProperties(finalClientId));) {
                for (Order order : orders) {
                    producer.send(new ProducerRecord<String, Order>(TOPIC_INCOMING_ORDERS + symbol, order));
                }
                producer.flush();
            }
        }
        log.debug("producer finished");
    }


    private Set<Order> generateFixedNumberOrders(final String symbol, int generetedTradeRequests, AtomicLong orderSeq,
                                                 AtomicLong userSeq) {
        final Random rnd = new Random(System.nanoTime());
        final Set<Order> result = new HashSet<>(generetedTradeRequests);
        for (int i = 0; i < generetedTradeRequests; i++) {
            final OrderOperation operation = OrderOperation.values()[rnd.nextInt(2)];
            final BigDecimal price = BigDecimal.valueOf(100 + rnd.nextDouble());
            final Order order = new Order(orderSeq.getAndIncrement(), userSeq.getAndIncrement(), symbol, operation,
                    rnd.nextInt(200), price, OrderType.Limit);
            result.add(order);
        }
        return result;
    }

    private Order generateOrder(String symbol, AtomicLong orderSeq, AtomicLong userSeq) {
        final Random rnd = new Random(System.nanoTime());
        final OrderOperation operation = OrderOperation.values()[rnd.nextInt(2)];
        final BigDecimal price = BigDecimal.valueOf(100 + rnd.nextDouble());
        final Order order = new Order(orderSeq.getAndIncrement(), userSeq.getAndIncrement(), symbol, operation,
                rnd.nextInt(200), price, OrderType.Limit);
        log.debug(order);
        return order;
    }

    class TimerProducer implements Runnable {
        final Random rnd = new Random(System.nanoTime());
        private final Producer<String, Order> producer;
        private final String symbol;
        private final AtomicLong orderSeq;
        private final AtomicLong userSeq;


        public TimerProducer(Producer<String, Order> producer, String symbol, AtomicLong orderSeq, AtomicLong userSeq) {
            this.producer = producer;
            this.symbol = symbol;
            this.orderSeq = orderSeq;
            this.userSeq = userSeq;
        }

        @Override
        public void run() {
            final Order order = generateOrder(symbol, orderSeq, userSeq);
            producer.send(new ProducerRecord<String, Order>(TOPIC_INCOMING_ORDERS + symbol, order));
            producer.flush();
        }
    }
}
