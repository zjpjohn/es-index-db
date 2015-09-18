package com.wxingyl.es.util;

/**
 * Created by xing on 15/9/9.
 * listener
 */
public interface Listener<T> {

    void onChange(T message);
}
