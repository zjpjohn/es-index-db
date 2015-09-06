package com.wxingyl.es.index;

import com.wxingyl.es.jdal.TableQueryResult;

import java.util.*;

/**
 * Created by xing on 15/9/6.
 * document, have page
 */
public class PageDocument<T extends DocFields> extends LinkedList<T> {

    private TableQueryResult.BaseInfo baseInfo;

    public PageDocument(TableQueryResult.BaseInfo baseInfo) {
        super();
        this.baseInfo = baseInfo;
    }

    public TableQueryResult.BaseInfo getBaseInfo() {
        return baseInfo;
    }

    public Map<Object, List<T>> groupByKeyField(boolean removeKeyField) {
        Map<Object, List<T>> group = new HashMap<>();
        forEach(doc -> {
            Object val = doc.get(baseInfo.getKeyField());
            List<T> list = group.get(val);
            if (list == null) {
                group.put(val, list = new LinkedList<>());
            }
            if (removeKeyField) doc.remove(baseInfo.getKeyField());
            list.add(doc);
        });
        return group;
    }

}
