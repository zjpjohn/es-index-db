package com.wxingyl.es.util.transfer;

import com.wxingyl.es.util.DefaultDateConvert;

import java.math.BigDecimal;
import java.util.Date;

/**
 * Created by xing on 15/9/30.
 * string value transfer builder
 */
public abstract class Transfers {

    @SuppressWarnings("unchecked")
    public static <T> StrValueTransfer<T> getTransfer(Class<T> cls) {
        if (cls == Integer.class || cls == Integer.TYPE) {
            return (StrValueTransfer<T>) IntegerStrValueTransfer.INSTANCE;
        } else if (cls == String.class) {
            return (StrValueTransfer<T>) StringStrValueTransfer.INSTANCE;
        } else if (cls == Long.class || cls == Long.TYPE) {
            return (StrValueTransfer<T>) LongStrValueTransfer.INSTANCE;
        } else if (cls == Double.class || cls == Double.TYPE) {
            return (StrValueTransfer<T>) DoubleStrValueTransfer.INSTANCE;
        } else if (cls == BigDecimal.class) {
            return (StrValueTransfer<T>) BigDecimalStrValueTransfer.INSTANCE;
        } else if (cls == Date.class) {
            return (StrValueTransfer<T>) DateStrValueTransfer.INSTANCE;
        } else {
            return null;
        }
    }

    public static StrValueTransfer<String> stringTransfer() {
        return StringStrValueTransfer.INSTANCE;
    }

    public static StrValueTransfer<Integer> intTransfer() {
        return IntegerStrValueTransfer.INSTANCE;
    }

    public static StrValueTransfer<Long> longTransfer() {
        return LongStrValueTransfer.INSTANCE;
    }

    public static StrValueTransfer<Double> doubleTransfer() {
        return DoubleStrValueTransfer.INSTANCE;
    }

    public static StrValueTransfer<BigDecimal> bigDecimalTransfer() {
        return BigDecimalStrValueTransfer.INSTANCE;
    }

    public static StrValueTransfer<Date> dateTransfer() {
        return DateStrValueTransfer.INSTANCE;
    }

    static abstract class AbstractStrValueTransfer<T extends Comparable<T>> implements StrValueTransfer<T> {
        @Override
        public int compare(T o1, T o2) {
            return o1.compareTo(o2);
        }
    }

    static class StringStrValueTransfer extends AbstractStrValueTransfer<String> {

        final static StringStrValueTransfer INSTANCE = new StringStrValueTransfer();

        @Override
        public String apply(String s) {
            return s;
        }
    }

    static class IntegerStrValueTransfer extends AbstractStrValueTransfer<Integer> {

        final static IntegerStrValueTransfer INSTANCE = new IntegerStrValueTransfer();

        @Override
        public Integer apply(String s) {
            return Integer.valueOf(s);
        }
    }

    static class LongStrValueTransfer extends AbstractStrValueTransfer<Long> {

        final static LongStrValueTransfer INSTANCE = new LongStrValueTransfer();

        @Override
        public Long apply(String s) {
            return Long.valueOf(s);
        }
    }

    static class DoubleStrValueTransfer extends AbstractStrValueTransfer<Double> {

        final static DoubleStrValueTransfer INSTANCE = new DoubleStrValueTransfer();

        @Override
        public Double apply(String s) {
            return Double.valueOf(s);
        }
    }

    static class BigDecimalStrValueTransfer extends AbstractStrValueTransfer<BigDecimal> {

        final static BigDecimalStrValueTransfer INSTANCE = new BigDecimalStrValueTransfer();

        @Override
        public BigDecimal apply(String s) {
            return new BigDecimal(s);
        }

    }

    static class DateStrValueTransfer extends AbstractStrValueTransfer<Date> {

        final static DateStrValueTransfer INSTANCE = new DateStrValueTransfer();

        @Override
        public Date apply(String s) {
            return DefaultDateConvert.INSTANCE.parse(s);
        }
    }
}
