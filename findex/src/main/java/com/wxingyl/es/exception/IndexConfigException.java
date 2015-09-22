package com.wxingyl.es.exception;

/**
 * Created by xing on 15/8/13.
 *  解析索引配置文件出错，一些必须配置的项缺失，或配置的值不合理等
 */
public class IndexConfigException extends RuntimeException {

    public IndexConfigException(String message) {
        super(message);
    }

    public IndexConfigException(String message, Throwable cause) {
        super(message, cause);
    }

}