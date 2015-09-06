package com.wxingyl.es.index;

import java.util.*;

/**
 * Created by xing on 15/9/6.
 * document, have page
 */
public class PageDocument extends LinkedList<PageDocument.DocAllFields> {

    /**
     * key: db field, value: newName field
     */
    private Map<String, String> fieldMap = null;

    /**
     * table key field
     */
    private String keyField;

    private String masterAlias;

    public PageDocument() {}

    /**
     * slave table query result
     * @param keyField child document key field
     * @param masterAlias child document add parent document key name
     */
    public PageDocument(String keyField, String masterAlias) {
        super();
        this.keyField = keyField;
        this.masterAlias = masterAlias;
    }

    public void addDocs(List<Map<String, Object>> data) {
        List<DocAllFields> list = new ArrayList<>(data.size());
        data.forEach(map -> {
            DocAllFields fields = new DocAllFields(map.size());
            fields.putAll(map);
            list.add(fields);
        });
        addAll(list);
    }

    public String getMasterAlias() {
        return masterAlias;
    }

    public void setFieldMap(Map<String, String> fieldMap) {
        this.fieldMap = fieldMap;
    }

    public Map<Object, List<DocAllFields>> groupByKeyField(boolean removeKeyField) {
        Map<Object, List<DocAllFields>> group = new HashMap<>();
        forEach(doc -> {
            Object val = doc.get(keyField);
            List<DocAllFields> list = group.get(val);
            if (list == null) {
                group.put(val, list = new LinkedList<>());
            }
            if (removeKeyField) doc.remove(keyField);
            list.add(doc);
        });
        return group;
    }

    public class DocAllFields {

        protected HashMap<String, Object> hashMap;

        public DocAllFields() {
            hashMap = new HashMap<>();
        }

        public DocAllFields(int initialCapacity) {
            hashMap = new HashMap<>(initialCapacity);
        }

        protected String getRealKey(Object key) {
            return fieldMap == null ? key.toString() : fieldMap.getOrDefault(key, key.toString());
        }

        public Object get(Object key) {
            return hashMap.get(getRealKey(key));
        }

        public Object put(String key, Object value) {
            return hashMap.put(getRealKey(key), value);
        }

        public void putAll(Map<? extends String, ?> m) {
            m.forEach(this::put);
        }

        public boolean containsKey(Object key) {
            return hashMap.containsKey(getRealKey(key));
        }

        public Object remove(Object key) {
            return hashMap.remove(getRealKey(key));
        }

    }
}
