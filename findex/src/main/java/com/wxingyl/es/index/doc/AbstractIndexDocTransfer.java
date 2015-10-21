package com.wxingyl.es.index.doc;

import com.wxingyl.es.db.DbTableDesc;
import com.wxingyl.es.db.result.TableQueryResult;
import com.wxingyl.es.exception.IndexIllegalArgumentException;
import com.wxingyl.es.index.IndexTypeBean;
import com.wxingyl.es.index.IndexTypeDesc;
import com.wxingyl.es.index.db.DbQueryDependResult;
import com.wxingyl.es.index.db.KeyFieldBatchDependQuery;
import com.wxingyl.es.index.db.TableDependQuery;
import com.wxingyl.es.index.db.TableQueryResultHandle;
import com.wxingyl.es.util.CommonUtils;

import java.util.*;

/**
 * Created by xing on 15/9/7.
 * define some register function
 */
public abstract class AbstractIndexDocTransfer implements IndexDocTransfer {

    private Map<IndexTypeDesc, DocPostProcessor> docPostProcessorMap = new HashMap<>();

    private Map<DbTableDesc, TableQueryResultHandle> tableQueryResultHandleMap = new HashMap<>();

    @Override
    public void registerDocPostProcessor(DocPostProcessor docPostProcessor) {
        if (CommonUtils.isEmpty(docPostProcessor.supportType())) {
            throw new IndexIllegalArgumentException("DocPostProcessor: " + docPostProcessor + " supportType is empty");
        }
        for (IndexTypeDesc v : docPostProcessor.supportType()) {
            if (v == null) return;
            docPostProcessorMap.put(v, docPostProcessor);
        }
    }

    @Override
    public void registerTableQueryResultHandle(TableQueryResultHandle handler) {
        if (CommonUtils.isEmpty(handler.supportTable())) {
            throw new IndexIllegalArgumentException("TableQueryResultListener: " + handler + " supportTable is empty");
        }
        for (DbTableDesc table : handler.supportTable()) {
            if (table == null) return;
            tableQueryResultHandleMap.put(table, handler);
        }
    }

    protected DocPostProcessor getDocPostProcessor(IndexTypeDesc type) {
        return docPostProcessorMap.get(type);
    }

    protected void notifyTableQueryResultHandler(IndexTypeDesc type, List<TableQueryResult> queryResults) {
        for (TableQueryResult q : queryResults) {
            TableQueryResultHandle handler = tableQueryResultHandleMap.get(q.getBaseInfo().getTable());
            if (handler != null) {
                handler.onHandle(type, q);
            }
        }
    }

    protected abstract class Itr implements PageDocumentIterator {

        protected final IndexTypeDesc type;

        protected final DocPostProcessor docPostProcessor;

        protected Itr(IndexTypeBean typeBean) {
            type = typeBean.getType();
            docPostProcessor = getDocPostProcessor(type);
        }

        @Override
        public void startFillIndex() {
            docPostProcessor.startPost(type);
        }

        @Override
        public void finishFillIndex() {
            docPostProcessor.endPost(type);
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("remove");
        }

    }
}
