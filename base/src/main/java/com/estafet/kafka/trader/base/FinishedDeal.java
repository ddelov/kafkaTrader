package com.estafet.kafka.trader.base;

import org.apache.log4j.Logger;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by Delcho Delov on 07.09.18.
 */
public class FinishedDeal {
    private static Logger log = Logger.getLogger(FinishedDeal.class);

    public final long id;
    public final long buyOrderId;
    public final long sellOrderId;
    public final int quantity;
    public final BigDecimal price;
    public final String symbol;//optional
    public final Date from;

    public FinishedDeal(long buyOrderId, long sellOrderId, int quantity, BigDecimal price, String symbol) {

        this.id = Constants.getFinishedDealId();
        this.buyOrderId = buyOrderId;
        this.sellOrderId = sellOrderId;
        this.quantity = quantity;
        this.price = price;
        this.symbol = symbol;
        this.from = new Date();
    }

    @Override
    public String toString() {
        final Date now = new Date();
        final SimpleDateFormat df = new SimpleDateFormat("HH:mm:ss");

        return String.format("%s: User %d buys %d %s shares x %s USD from seller %d",
                df.format(now), buyOrderId, quantity, symbol, price.toString(), sellOrderId);
    }
}
