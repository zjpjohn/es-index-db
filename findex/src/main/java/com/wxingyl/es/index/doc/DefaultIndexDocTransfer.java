package com.wxingyl.es.index.doc;

import com.wxingyl.es.db.result.TableQueryResult;
import com.wxingyl.es.index.IndexTypeBean;
import com.wxingyl.es.index.IndexTypeDesc;
import com.wxingyl.es.index.db.DbQueryDependResult;
import com.wxingyl.es.index.db.KeyFieldBatchDependQuery;
import com.wxingyl.es.index.db.TableDependQuery;

import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Created by xing on 15/9/6.
 * should singleton obj
 */
public class DefaultIndexDocTransfer extends AbstractIndexDocTransfer {

    private DocPostProcessor defaultDocPostProcessor;

    public DefaultIndexDocTransfer() {
        defaultDocPostProcessor = new DefaultDocPostProcessor();
    }

    @Override
    public PageDocumentIterator indexDocCreate(IndexTypeBean typeBean, int startPage, int endPage) {
        return new DefaultItr(typeBean, startPage, endPage);
    }

    @Override
    public PageDocument indexDocCreate(IndexTypeBean typeBean, TableQueryResult queryResult) {
        KeyFieldBatchDependQuery dependQuery = new KeyFieldBatchDependQuery(typeBean.getMasterTable()
                .getTableQueryBean(queryResult.getBaseInfo().getTable()));
        return transferDoc(typeBean.getType(), dependQuery.query(queryResult), getDocPostProcessor(typeBean.getType()));
    }

    @Override
    public void registerDocPostProcessor(DocPostProcessor docPostProcessor) {
        super.registerDocPostProcessor(new DocPostProcessorWrapper(docPostProcessor));
    }

    @Override
    protected DocPostProcessor getDocPostProcessor(IndexTypeDesc type) {
        DocPostProcessor processor = super.getDocPostProcessor(type);
        return processor == null ? defaultDocPostProcessor : processor;
    }

    private PageDocument transferDoc(IndexTypeDesc type, DbQueryDependResult dependResult, DocPostProcessor docPostProcessor) {
        notifyTableQueryResultHandler(type, dependResult.getAllTableResult());

        PageDocument pageDocument = docPostProcessor.postProcessor(dependResult);

        if (pageDocument == null) {
            if (dependResult.getSlaveResult() == null) {
                pageDocument = docPostProcessor.initMasterPageDoc(dependResult.getTableQueryResult());
            } else {
                pageDocument = document(null, dependResult, docPostProcessor);
            }
        }
        return pageDocument;
    }

    private PageDocument document(PageDocument pageDocument, DbQueryDependResult queryResult, DocPostProcessor docPostProcessor) {
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
                PageDocument childDocument = document(null, result, docPostProcessor);
                if (childDocument != null) {
                    pageDocument = docPostProcessor.mergeChildPageDoc(pageDocument, masterField, childDocument);
                }
            }
        }
        return pageDocument;
    }

    class DefaultItr extends Itr {

        private final Iterator<DbQueryDependResult> query;

        DefaultItr(IndexTypeBean typeBean, int startPage, int endPage) {
            super(typeBean);
            query = new TableDependQuery(typeBean.getMasterTable(), startPage, endPage);
        }

        @Override
        public boolean hasNext() {
            return query.hasNext();
        }

        @Override
        public PageDocument next() {
            return transferDoc(type, query.next(), docPostProcessor);
        }

    }

    private class DocPostProcessorWrapper implements DocPostProcessor {

        DocPostProcessor postProcessor;

        DocPostProcessorWrapper(DocPostProcessor postProcessor) {
            this.postProcessor = postProcessor;
        }

        @Override
        public void startPost(IndexTypeDesc type) {
            postProcessor.startPost(type);
            defaultDocPostProcessor.startPost(type);
        }

        @Override
        public void endPost(IndexTypeDesc type) {
            postProcessor.endPost(type);
            defaultDocPostProcessor.endPost(type);
        }

        @Override
        public PageDocument postProcessor(DbQueryDependResult masterResult) {
            return postProcessor.postProcessor(masterResult);
        }

        @Override
        public PageDocument initMasterPageDoc(TableQueryResult masterResult) {
            PageDocument pageDocument = postProcessor.initMasterPageDoc(masterResult);
            if (pageDocument == null) {
                pageDocument = defaultDocPostProcessor.initMasterPageDoc(masterResult);
            }
            return pageDocument;
        }

        @Override
        public PageDocument applyTableQueryResult(PageDocument masterPageDoc, String masterField, TableQueryResult slaveResult) {
            PageDocument pageDocument = postProcessor.applyTableQueryResult(masterPageDoc, masterField, slaveResult);
            if (pageDocument == null) {
                pageDocument = defaultDocPostProcessor.applyTableQueryResult(masterPageDoc, masterField, slaveResult);
            }
            return pageDocument;
        }

        @Override
        public PageDocument mergeChildPageDoc(PageDocument masterPageDoc, String masterField, PageDocument childPageDoc) {
            PageDocument pageDocument = postProcessor.mergeChildPageDoc(masterPageDoc, masterField, childPageDoc);
            if (pageDocument == null) {
                pageDocument = defaultDocPostProcessor.mergeChildPageDoc(masterPageDoc, masterField, childPageDoc);
            }
            return pageDocument;
        }

        @Override
        public Set<IndexTypeDesc> supportType() {
            return postProcessor.supportType();
        }
    }
}
