package com.wxingyl.es.util.transfer;

import org.elasticsearch.common.base.Function;

import java.util.Comparator;

/**
 * Created by xing on 15/9/30.
 * transfer String value to T
 */
public interface StrValueTransfer<T> extends Function<String, T>, Comparator<T> {

}
