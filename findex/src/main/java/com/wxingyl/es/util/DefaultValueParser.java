package com.wxingyl.es.util;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by xing on 15/8/24.
 * level default value parse
 */
public class DefaultValueParser<T> {

    private Map<String, T[]> defaultValue = new HashMap<>();

    private Map<String, String> keyMap = new HashMap<>();

    public DefaultValueParser(BiConsumer<Map<String, T[]>, Map<String, String>> init) {
        init.accept(defaultValue, keyMap);
    }

    /**
     * add default value
     * @param level start from 0
     */
    public void addDefaultValue(Map<String, Object> confMap, int level) {
        for (Map.Entry<String, T[]> e : defaultValue.entrySet()) {
            String k = e.getKey();
            T[] v = e.getValue();
            T val = getVal(confMap, k);
            if (val != null) {
                v[level] = val;
            } else if (level > 0) {
                v[level] = v[level - 1];
            } else {
                v[0] = null;
            }
            if (confMap.containsKey(k)) confMap.remove(k);
        }
    }

    @SuppressWarnings("unchecked")
    protected T getVal(Map<String, Object> confMap, String key) {
        return (T) confMap.get(key);
    }

    /**
     * get final value, if value is null, will get default value from upper level
     */
    public Map<String, T> getDefaultValue(Map<String, Object> confMap) {
        Map<String, T> value = new HashMap<>();
        for (Map.Entry<String, String> e : keyMap.entrySet()) {
            String k = e.getKey();
            String v = e.getValue();
            T val = getVal(confMap, v);
            if (val == null) {
                T[] array = defaultValue.get(k);
                val = array[array.length-1];
            }
            if (val != null || confMap.containsKey(v)) {
                value.put(v, val);
                confMap.remove(v);
            }
        }
        return value;
    }
}
