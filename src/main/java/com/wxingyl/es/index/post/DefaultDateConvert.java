package com.wxingyl.es.index.post;

import com.wxingyl.es.util.DateConvert;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by xing on 15/9/7.
 * default date convert
 */
public class DefaultDateConvert implements DateConvert {

    private static final ThreadLocal<SimpleDateFormat> DATE_FORMAT = new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        }
    };

    public static final DefaultDateConvert INSTANCE = new DefaultDateConvert();

    private DefaultDateConvert() {}

    @Override
    public String format(Date date) {
        return DATE_FORMAT.get().format(date);
    }

    @Override
    public String format(long timestamp) {
        return DATE_FORMAT.get().format(new Date(timestamp));
    }

    @Override
    public Date parse(String dateStr) {
        try {
            return DATE_FORMAT.get().parse(dateStr);
        } catch (ParseException e) {
            return null;
        }
    }
}
