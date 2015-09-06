package com.wxingyl.es.index;

import java.util.*;

/**
 * Created by xing on 15/9/6.
 * document, have page
 */
public class PageDocument<T extends DocFields> extends LinkedList<T> {
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

    public String getMasterAlias() {
        return masterAlias;
    }

    public Map<Object, List<T>> groupByKeyField(boolean removeKeyField) {
        Map<Object, List<T>> group = new HashMap<>();
        forEach(doc -> {
            Object val = doc.get(keyField);
            List<T> list = group.get(val);
            if (list == null) {
                group.put(val, list = new LinkedList<>());
            }
            if (removeKeyField) doc.remove(keyField);
            list.add(doc);
        });
        return group;
    }

}
