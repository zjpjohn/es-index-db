package com.wxingyl.es;

import com.wxingyl.es.conf.index.IndexTypeBean;
import com.wxingyl.es.dbquery.DbTableDesc;
import com.wxingyl.es.dbquery.TableQueryResult;
import com.wxingyl.es.index.IndexTypeDesc;
import com.wxingyl.es.index.doc.AbstractDocPostProcessor;
import com.wxingyl.es.index.doc.PageDocument;

import java.util.*;

/**
 * Created by xing on 15/9/9.
 * order info
 */
public class OrderTypeDocPostProcessor extends AbstractDocPostProcessor {

    private Set<IndexTypeDesc> supportTypes;

    private DbTableDesc warehouseTable;

    public OrderTypeDocPostProcessor(IndexTypeBean type) {
        Set<IndexTypeDesc> typeSet = new HashSet<>();
        typeSet.add(type.getType());
        supportTypes = Collections.unmodifiableSet(typeSet);
        warehouseTable = type.getTableInfo("db_warehouse").get(0).getTable();
    }

    @Override
    @SuppressWarnings("unchecked")
    public PageDocument applyTableQueryResult(PageDocument masterPageDoc, String masterField, TableQueryResult slaveResult) {
        if (!slaveResult.getBaseInfo().getTable().equals(warehouseTable)) return null;
        Map<Long, String> warehouseMap = new HashMap<>();
        String warehouseIdField = slaveResult.getBaseInfo().getKeyField();
        slaveResult.getDbData().forEach(f -> {
            warehouseMap.put((Long) f.get(warehouseIdField), (String) f.get("warehouse_name"));
        });
        masterPageDoc.forEach(doc -> {
            doc.put("warehouse_name", warehouseMap.get(doc.get(masterField)));
        });
        return masterPageDoc;
    }

    @Override
    public PageDocument mergeChildPageDoc(PageDocument masterPageDoc, String masterField, PageDocument childPageDoc) {
        return null;
    }

    @Override
    public Set<IndexTypeDesc> supportType() {
        return supportTypes;
    }
}
