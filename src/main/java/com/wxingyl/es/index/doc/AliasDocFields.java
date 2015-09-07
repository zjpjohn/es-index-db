package com.wxingyl.es.index.doc;

import java.util.*;

/**
 * Created by xing on 15/9/6.
 * field support alias
 */
public class AliasDocFields extends DocFields {

    private Map<String, String> aliasMap;

    public AliasDocFields(Map<String, String> aliasMap, int initialCapacity) {
        super(initialCapacity);
        this.aliasMap = aliasMap;
    }

    @Override
    public Object get(String key) {
        return super.get(aliasMap.getOrDefault(key, key));
    }

    @Override
    public Object put(String key, Object value) {
        return super.put(aliasMap.getOrDefault(key, key), value);
    }

    @Override
    public void putAll(Map<? extends String, ?> m) {
        m.forEach(this::put);
    }

    @Override
    public Object remove(String key) {
        return super.remove(aliasMap.getOrDefault(key, key));
    }

    @Override
    public boolean containsKey(String key) {
        return super.containsKey(aliasMap.getOrDefault(key, key));
    }

    public Map<String, String> getAliasMap() {
        return aliasMap;
    }

    public static List<AliasDocFields> build(Map<String, String> aliasMap, List<Map<String, Object>> data) {
        List<AliasDocFields> list = new ArrayList<>(data.size());
        data.forEach(map -> {
            AliasDocFields fields = new AliasDocFields(aliasMap, map.size());
            fields.putAll(map);
            list.add(fields);
        });
        return list;
    }

}
