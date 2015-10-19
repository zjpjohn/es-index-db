package com.wxingyl.es.util.transfer;

import java.util.Comparator;

/**
 * Created by xing on 15/9/30.
 * transfer String value to T
 */
public interface StrValueConvert<T> extends Comparator<T> {

    T convert(String input);

}
