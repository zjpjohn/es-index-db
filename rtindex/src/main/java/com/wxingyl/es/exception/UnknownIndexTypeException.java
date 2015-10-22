package com.wxingyl.es.exception;

import com.wxingyl.es.index.IndexTypeDesc;

/**
 * Created by xing on 15/10/19.
 * can not find index/type
 */
public class UnknownIndexTypeException extends RuntimeException {

    public UnknownIndexTypeException(IndexTypeDesc type) {
        super("Can't find type: " + type.toString());
    }

}
