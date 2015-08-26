package com.wxingyl.es.exception;

/**
 * Created by xing on 15/8/17.
 * 数据源配置错误异常
 */
public class DataSourceConfigException extends RuntimeException {

    public DataSourceConfigException(String message) {
        super(message);
    }

    public DataSourceConfigException(String message, Throwable cause) {
        super(message, cause);
    }
}
