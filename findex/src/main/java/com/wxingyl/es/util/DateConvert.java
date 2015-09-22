package com.wxingyl.es.util;

import java.util.Date;

/**
 * Created by xing on 15/9/7.
 * date time convert
 */
public interface DateConvert {

    String format(Date date);

    /**
     * @param timestamp unit is millisecond
     */
    String format(long timestamp);

    Date parse(String dateStr);
}
