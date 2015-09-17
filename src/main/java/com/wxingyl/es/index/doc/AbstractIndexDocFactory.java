package com.wxingyl.es.index.doc;

import com.wxingyl.es.db.DbTableDesc;
import com.wxingyl.es.db.result.TableQueryResult;
import com.wxingyl.es.exception.IndexIllegalArgumentException;
import com.wxingyl.es.index.IndexTypeBean;
import com.wxingyl.es.index.IndexTypeDesc;
import com.wxingyl.es.index.db.DbQueryDependResult;
import com.wxingyl.es.index.db.TableDependQuery;
import com.wxingyl.es.index.db.TableQueryResultHandle;
import com.wxingyl.es.util.CommonUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Created by xing on 15/9/7.
 * define some register function
 */
public abstract class AbstractIndexDocFactory implements IndexDocFactory {

    private Map<IndexTypeDesc, DocPostProcessor> docPostProcessorMap = new HashMap<>();

    private Map<DbTableDesc, TableQueryResultHandle> tableQueryResultHandleMap = new HashMap<>();

    @Override
    public void registerDocPostProcessor(DocPostProcessor docPostProcessor) {
        if (CommonUtils.isEmpty(docPostProcessor.supportType())) {
            throw new IndexIllegalArgumentException("DocPostProcessor: " + docPostProcessor + " supportType is empty");
        }
        docPostProcessor.supportType().forEach(v -> {
            if (v == null) return;
            docPostProcessorMap.put(v, docPostProcessor);
        });
    }

    @Override
    public void registerTableQueryResultHandle(TableQueryResultHandle handler) {
        if (CommonUtils.isEmpty(handler.supportTable())) {
            throw new IndexIllegalArgumentException("TableQueryResultListener: " + handler + " supportTable is empty");
        }
        handler.supportTable().forEach(table -> {
            if (table == null) return;
            tableQueryResultHandleMap.put(table, handler);
        });
    }

    protected DocPostProcessor getDocPostProcessor(IndexTypeDesc type) {
        return docPostProcessorMap.get(type);
    }

    protected void notifyTableQueryResultHandler(IndexTypeDesc type, List<TableQueryResult> queryResults) {
        queryResults.forEach(q -> {
            TableQueryResultHandle handler = tableQueryResultHandleMap.get(q.getBaseInfo().getTable());
            if (handler != null) {
                handler.onHandle(type, q);
            }
        });
    }

    protected abstract class Itr implements PageDocumentIterator {

        private IndexTypeDesc type;

        private TableDependQuery query;

        protected DocPostProcessor docPostProcessor;

        protected Itr(IndexTypeBean typeBean, int startPage) {
            type = typeBean.getType();
            query = new TableDependQuery(typeBean.getMasterTable(), startPage);
            docPostProcessor = getDocPostProcessor(type);
        }

        @Override
        public boolean hasNext() {
            return query.hasNext();
        }

        @Override
        public PageDocument next() {
            DbQueryDependResult ret = query.next();
            notifyTableQueryResultHandler(type, ret.getAllTableResult());

            PageDocument pageDocument = docPostProcessor.postProcessor(ret);

            if (pageDocument == null) {
                if (ret.getSlaveResult() == null) {
                    pageDocument = docPostProcessor.initMasterPageDoc(ret.getTableQueryResult());
                } else {
                    pageDocument = document(null, ret);
                }
            }
            return pageDocument;
        }

        @Override
        public void startFillIndex() {
            docPostProcessor.startPost(type);
        }

        @Override
        public void finishFillIndex() {
            docPostProcessor.endPost(type);
        }

        protected PageDocument document(PageDocument pageDocument, DbQueryDependResult queryResult) {
            for (Map.Entry<String, DbQueryDependResult> e : queryResult.getSlaveResult().entries()) {
                DbQueryDependResult result = e.getValue();
                if (pageDocument == null) {
                    pageDocument = docPostProcessor.initMasterPageDoc(queryResult.getTableQueryResult());
                }
                Objects.requireNonNull(pageDocument);
                String masterField = e.getKey();
                if (result.getSlaveResult() == null) {
                    pageDocument = docPostProcessor.applyTableQueryResult(pageDocument, masterField, result.getTableQueryResult());
                } else {
                    PageDocument childDocument = document(null, result);
                    if (childDocument != null) {
                        pageDocument = docPostProcessor.mergeChildPageDoc(pageDocument, masterField, childDocument);
                    }
                }
            }
            return pageDocument;
        }

    }
}
