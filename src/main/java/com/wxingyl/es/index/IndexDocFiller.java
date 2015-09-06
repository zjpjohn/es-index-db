package com.wxingyl.es.index;

import com.wxingyl.es.conf.index.IndexTypeBean;

import java.util.Map;
import java.util.Objects;

/**
 * Created by xing on 15/8/30.
 * index document filler
 */
public class IndexDocFiller implements IndexDocFill {

    private IndexTypeManager indexTypeManager;

    private DocPostProcessor defaultDocPostProcessor;

    @Override
    public void fill(IndexTypeBean typeBean) {
        IndexTypeDesc type = typeBean.getType();
        TableDependQuery query = new TableDependQuery(typeBean.getMasterTable());
        while (query.hasNext()) {
            DbQueryDependResult ret = query.next();
            indexTypeManager.notifyTableQueryResultListener(type, ret.getAllTableResult());
            DocPostProcessor docPostProcessor = indexTypeManager.getDocPostProcessor(type);
            PageDocument pageDocument = null;
            if (docPostProcessor != null) {
                pageDocument = docPostProcessor.postProcessor(ret);
            } else {
                docPostProcessor = defaultDocPostProcessor;
            }
            if (pageDocument == null) {
                pageDocument = document(docPostProcessor, null, ret);
            }
        }
    }

    @Override
    public void setIndexTypeManager(IndexTypeManager indexTypeManager) {
        this.indexTypeManager = indexTypeManager;
    }

    @Override
    public void setDefaultDocPostProcessor(DocPostProcessor defaultDocPostProcessor) {
        this.defaultDocPostProcessor = defaultDocPostProcessor;
    }

    private PageDocument document(DocPostProcessor docPostProcessor, PageDocument pageDocument, DbQueryDependResult queryResult) {
        for (Map.Entry<String, DbQueryDependResult> e : queryResult.getSlaveResult().entries()) {
            DbQueryDependResult result = e.getValue();
            if (pageDocument == null) {
                if ((pageDocument = docPostProcessor.initMasterPageDoc(queryResult.getTableQueryResult())) == null
                        && docPostProcessor != defaultDocPostProcessor) {
                    pageDocument = defaultDocPostProcessor.initMasterPageDoc(queryResult.getTableQueryResult());
                }
            }
            Objects.requireNonNull(pageDocument);
            String masterField = e.getKey();
            if (result.getSlaveResult() == null) {
                PageDocument postPageDocument = docPostProcessor.applyTableQueryResult(pageDocument, masterField, result.getTableQueryResult());
                if (postPageDocument == null && docPostProcessor != defaultDocPostProcessor) {
                    postPageDocument = defaultDocPostProcessor.applyTableQueryResult(pageDocument, masterField, result.getTableQueryResult());
                }
                pageDocument = postPageDocument;
            } else {
                PageDocument childDocument = document(docPostProcessor, null, result);
                if (childDocument != null) {
                    PageDocument postPageDocument = docPostProcessor.mergeChildPageDoc(pageDocument, masterField, childDocument);
                    if (postPageDocument == null && docPostProcessor != defaultDocPostProcessor) {
                        postPageDocument = defaultDocPostProcessor.mergeChildPageDoc(pageDocument, masterField, childDocument);
                    }
                    pageDocument = postPageDocument;
                }
            }
        }
        return pageDocument;
    }

}
