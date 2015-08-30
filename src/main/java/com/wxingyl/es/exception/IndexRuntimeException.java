package com.wxingyl.es.exception;

/**
 * Created by xing on 15/8/31.
 * fill index data have some runtime exception
 */
public class IndexRuntimeException extends RuntimeException {

    public IndexRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }
}
