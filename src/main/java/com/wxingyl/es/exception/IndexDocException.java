package com.wxingyl.es.exception;

/**
 * Created by xing on 15/8/31.
 * fill index data have some runtime exception
 * fill doc to index have crash
 */
public class IndexDocException extends RuntimeException {

    public IndexDocException(String message, Throwable cause) {
        super(message, cause);
    }
}
