package com.wxingyl.es.util.transfer;

import com.wxingyl.es.util.DefaultDateConvert;

import java.math.BigDecimal;
import java.util.Date;

/**
 * Created by xing on 15/9/30.
 * string value transfer builder
 */
public abstract class StrValConverts {

    @SuppressWarnings("unchecked")
    public static <T> StrValueConvert<T> getConvert(Class<T> cls) {
        if (cls == Integer.class || cls == Integer.TYPE) {
            return (StrValueConvert<T>) IntegerStrValueConvert.INSTANCE;
        } else if (cls == String.class) {
            return (StrValueConvert<T>) StringStrValueConvert.INSTANCE;
        } else if (cls == Long.class || cls == Long.TYPE) {
            return (StrValueConvert<T>) LongStrValueConvert.INSTANCE;
        } else if (cls == Double.class || cls == Double.TYPE) {
            return (StrValueConvert<T>) DoubleStrValueConvert.INSTANCE;
        } else if (cls == BigDecimal.class) {
            return (StrValueConvert<T>) BigDecimalStrValueConvert.INSTANCE;
        } else if (cls == Date.class) {
            return (StrValueConvert<T>) DateStrValueConvert.INSTANCE;
        } else {
            return null;
        }
    }

    public static StrValueConvert<String> stringConvert() {
        return StringStrValueConvert.INSTANCE;
    }

    public static StrValueConvert<Integer> intConvert() {
        return IntegerStrValueConvert.INSTANCE;
    }

    public static StrValueConvert<Long> longConvert() {
        return LongStrValueConvert.INSTANCE;
    }

    public static StrValueConvert<Double> doubleConvert() {
        return DoubleStrValueConvert.INSTANCE;
    }

    public static StrValueConvert<BigDecimal> bigDecimalConvert() {
        return BigDecimalStrValueConvert.INSTANCE;
    }

    public static StrValueConvert<Date> dateConvert() {
        return DateStrValueConvert.INSTANCE;
    }

    static abstract class AbstractStrValueConvert<T extends Comparable<T>> implements StrValueConvert<T> {
        @Override
        public int compare(T o1, T o2) {
            return o1.compareTo(o2);
        }
    }

    static class StringStrValueConvert extends AbstractStrValueConvert<String> {

        final static StringStrValueConvert INSTANCE = new StringStrValueConvert();

        @Override
        public String convert(String s) {
            return s;
        }
    }

    static class IntegerStrValueConvert extends AbstractStrValueConvert<Integer> {

        final static IntegerStrValueConvert INSTANCE = new IntegerStrValueConvert();

        @Override
        public Integer convert(String s) {
            return Integer.valueOf(s);
        }
    }

    static class LongStrValueConvert extends AbstractStrValueConvert<Long> {

        final static LongStrValueConvert INSTANCE = new LongStrValueConvert();

        @Override
        public Long convert(String s) {
            return Long.valueOf(s);
        }
    }

    static class DoubleStrValueConvert extends AbstractStrValueConvert<Double> {

        final static DoubleStrValueConvert INSTANCE = new DoubleStrValueConvert();

        @Override
        public Double convert(String s) {
            return Double.valueOf(s);
        }
    }

    static class BigDecimalStrValueConvert extends AbstractStrValueConvert<BigDecimal> {

        final static BigDecimalStrValueConvert INSTANCE = new BigDecimalStrValueConvert();

        @Override
        public BigDecimal convert(String s) {
            return new BigDecimal(s);
        }

    }

    static class DateStrValueConvert extends AbstractStrValueConvert<Date> {

        final static DateStrValueConvert INSTANCE = new DateStrValueConvert();

        @Override
        public Date convert(String s) {
            return DefaultDateConvert.INSTANCE.parse(s);
        }
    }
}
