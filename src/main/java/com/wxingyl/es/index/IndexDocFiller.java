package com.wxingyl.es.index;

import com.wxingyl.es.conf.index.IndexTypeBean;
import com.wxingyl.es.jdal.DbTableDesc;
import com.wxingyl.es.jdal.TableQueryResult;
import com.wxingyl.es.util.CommonUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by xing on 15/8/30.
 * index document filler
 */
public class IndexDocFiller implements IndexDocFill {

    private Map<DbTableDesc, TableQueryResultListener> tableQueryResultListenerMap = new HashMap<>();

    @Override
    public void fill(IndexTypeBean typeBean) {
        IndexTypeDesc type = typeBean.getType();
        TableDependQuery query = new TableDependQuery(typeBean.getMasterTable());
        while (query.hasNext()) {
            DbQueryDependResult ret = query.next();
            PageDocument pageDocument = document(null, ret);
        }
    }

    private PageDocument document(PageDocument pageDocument, DbQueryDependResult queryResult) {
        for (Map.Entry<String, DbQueryDependResult> e : queryResult.getSlaveResult().entries()) {
            DbQueryDependResult result = e.getValue();
            if (result.getSlaveResult() == null)
                pageDocument = applyTableQueryResult(pageDocument, e.getKey(), queryResult.getTableQueryResult(),
                        result.getTableQueryResult());
            else {
                PageDocument childDocument = document(null, result);
                if (childDocument != null) {
                    pageDocument = mergeChildPageDoc(pageDocument, e.getKey(), queryResult.getTableQueryResult(), childDocument);
                }
            }
        }
        return pageDocument;
    }

    private PageDocument mergeChildPageDoc(PageDocument masterPageDoc, String masterField,
                                           TableQueryResult masterResult, PageDocument childPageDoc) {
        if (masterPageDoc == null) {
            masterPageDoc = initMasterPageDoc(masterResult);
        }
        Map<Object, List<PageDocument.DocAllFields>> group = childPageDoc.groupByKeyField(true);
        String masterAlias = childPageDoc.getMasterAlias();
        masterPageDoc.forEach(doc -> {
            Object val = doc.get(masterField);
            if (val != null && group.get(val) != null) {
                doc.put(masterAlias, group.get(val));
            }
        });
        return masterPageDoc;
    }

    private PageDocument initMasterPageDoc(TableQueryResult masterResult) {
        PageDocument masterPageDoc = new PageDocument(masterResult.getKeyField(), masterResult.getMasterAlias());
        masterPageDoc.addDocs(masterResult.getDbData());
        return masterPageDoc;
    }

    private PageDocument applyTableQueryResult(PageDocument masterPageDoc, String masterField,
                                               TableQueryResult masterResult, TableQueryResult slaveResult) {
        if (masterPageDoc == null) {
            masterPageDoc = initMasterPageDoc(masterResult);
        }
        Map<Object, List<Map<String, Object>>> group = CommonUtils.groupListMap(slaveResult.getDbData(),
                slaveResult.getKeyField(), true);
        String keyAlias = slaveResult.getMasterAlias();
        masterPageDoc.forEach(doc -> {
            Object obj = doc.get(masterField);
            if (obj != null && group.get(obj) != null) {
                doc.put(keyAlias, group.get(obj));
            }
        });
        return masterPageDoc;
    }

    @Override
    public boolean addTableQueryResultListener(TableQueryResultListener listener) {
        for (DbTableDesc table : listener.supportTable()) {
            if (tableQueryResultListenerMap.containsKey(table)) {
                throw new IllegalArgumentException("table: " + table + " has TableQueryResultListener: "
                        + tableQueryResultListenerMap.get(table) + ", support tables: " + tableQueryResultListenerMap.get(table).supportTable());
            } else {
                tableQueryResultListenerMap.put(table, listener);
            }
        }
        return true;
    }

}
