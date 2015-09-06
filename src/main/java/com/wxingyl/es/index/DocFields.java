package com.wxingyl.es.index;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by xing on 15/9/6.
 * document
 */
public class DocFields {

    private HashMap<String, Object> hashMap;

    public DocFields() {
        hashMap = new HashMap<>();
    }

    public DocFields(int initialCapacity) {
        hashMap = new HashMap<>(initialCapacity);
    }

    public Object get(String key) {
        return hashMap.get(key);
    }

    public Object put(String key, Object value) {
        return hashMap.put(key, value);
    }

    public void putAll(Map<? extends String, ?> m) {
        hashMap.putAll(m);
    }

    public Object remove(String key) {
        return hashMap.remove(key);
    }

    public boolean containsKey(String key) {
        return hashMap.containsKey(key);
    }

    public HashMap<String, Object> getHashMap() {
        return hashMap;
    }

    public static List<DocFields> build(List<Map<String, Object>> data) {
        List<DocFields> list = new ArrayList<>(data.size());
        data.forEach(map -> {
            DocFields fields = new DocFields(map.size());
            fields.putAll(map);
            list.add(fields);
        });
        return list;
    }
}
