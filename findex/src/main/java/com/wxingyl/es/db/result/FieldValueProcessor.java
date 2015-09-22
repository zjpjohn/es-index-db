package com.wxingyl.es.db.result;

/**
 * Created by xing on 15/9/9.
 * query db result, handle field value of row
 */
public interface FieldValueProcessor {

    /**
     * @param fieldName field name
     * @param value this field name
     * @return new value, in most cases equals the value of param, don't have change
     */
    Object handle(String fieldName, Object value);
}
