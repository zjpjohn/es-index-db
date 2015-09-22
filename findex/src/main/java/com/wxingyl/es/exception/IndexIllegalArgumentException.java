package com.wxingyl.es.exception;

/**
 * Created by xing on 15/9/7.
 * index operation, argument is illegal
 */
public class IndexIllegalArgumentException extends RuntimeException {

    public IndexIllegalArgumentException(String message) {
        super(message);
    }
}
