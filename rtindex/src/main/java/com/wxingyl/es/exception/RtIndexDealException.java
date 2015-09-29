package com.wxingyl.es.exception;

/**
 * Created by xing on 15/9/24.
 * real time index deal exception
 */
public class RtIndexDealException extends RuntimeException {

    public RtIndexDealException(String message) {
        super(message);
    }

    public RtIndexDealException(String message, Throwable cause) {
        super(message, cause);
    }

}
