package com.wxingyl.es.db.result;

/**
 * Created by xing on 15/9/10.
 * db query result, the same type of number may be have different Class type, such Integer and Long
 * Note: default we only handle Integer to Long. Because float, double, BigDecimal used Map.key is rare, so we don't care
 */
public class NumberFieldValueProcessor implements FieldValueProcessor {

    public static final NumberFieldValueProcessor INSTANCE = new NumberFieldValueProcessor();

    /**
     * @param fieldName field name, here can null
     */
    @Override
    public Object handle(String fieldName, Object value) {
        if (!(value instanceof Number)) return value;
        Class cls = value.getClass();
        if (Integer.class == cls || Short.class == cls || Byte.class == cls) {
            return ((Number) value).longValue();
        } else {
            return value;
        }
    }

}
