package com.wxingyl.es.exception;

/**
 * Created by xing on 15/10/23.
 * deal real time data change, doc of Map<String, Object> convert to json have Exception
 */
public class RtDocConvertJsonException extends RuntimeException {

    public RtDocConvertJsonException(String message, Throwable cause) {
        super(message, cause);
    }
}
