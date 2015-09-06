package com.wxingyl.es.index;

import com.wxingyl.es.exception.IndexDocException;
import com.wxingyl.es.jdal.DbTableDesc;
import com.wxingyl.es.jdal.TableQueryResult;
import com.wxingyl.es.util.CommonUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Created by xing on 15/9/6.
 * should singleton obj
 */
public class IndexTypeManager {

    private Map<IndexTypeDesc, DocPostProcessor> docPostProcessorMap = new HashMap<>();

    private Map<DbTableDesc, TableQueryResultListener> tableQueryResultListenerMap = new HashMap<>();

    private DocPostProcessor defaultDocPostProcessor = new DefaultDocPostProcessor();

    public DocPostProcessor getDocPostProcessor(IndexTypeDesc type) {
        return docPostProcessorMap.get(type);
    }

    public void registerDocPostProcessor(DocPostProcessor docPostProcessor) {
        Objects.requireNonNull(docPostProcessor.supportType(), () -> "docPostProcessor supportType can not null");
        docPostProcessorMap.put(docPostProcessor.supportType(), docPostProcessor);
    }

    public void notifyTableQueryResultListener(IndexTypeDesc type, List<TableQueryResult> queryResults) {
        queryResults.forEach(q -> {
            TableQueryResultListener listener = tableQueryResultListenerMap.get(q.getTable());
            if (listener != null) {
                listener.onHandle(type, q);
            }
        });
    }

    public void registerTableQueryResultListener(TableQueryResultListener listener) {
        if (CommonUtils.isEmpty(listener.supportTable())) {
            throw new IndexDocException("TableQueryResultListener: " + listener + " supportTable is empty");
        }
        listener.supportTable().forEach(table -> tableQueryResultListenerMap.put(table, listener));
    }
}
