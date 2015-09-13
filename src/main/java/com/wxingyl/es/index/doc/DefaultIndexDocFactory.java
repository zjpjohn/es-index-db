package com.wxingyl.es.index.doc;

import com.wxingyl.es.db.result.TableQueryResult;
import com.wxingyl.es.index.IndexTypeBean;
import com.wxingyl.es.index.IndexTypeDesc;
import com.wxingyl.es.index.db.DbQueryDependResult;

import java.util.Set;

/**
 * Created by xing on 15/9/6.
 * should singleton obj
 */
public class DefaultIndexDocFactory extends AbstractIndexDocFactory {

    private DocPostProcessor defaultDocPostProcessor;

//    private BulkIndexGenerate defaultBulkIndexGenerator;

    public DefaultIndexDocFactory() {
        defaultDocPostProcessor = new DefaultDocPostProcessor();
//        defaultBulkIndexGenerator = new DefaultBulkIndexGenerator();
    }

    @Override
    public PageDocumentIterator indexDocCreate(IndexTypeBean typeBean) {
        return new DefaultItr(typeBean);
    }

    class DefaultItr extends Itr {

        DefaultItr(IndexTypeBean typeBean) {
            super(typeBean);
            if (docPostProcessor == null) {
                docPostProcessor = defaultDocPostProcessor;
            }
        }

    }

    @Override
    public void registerDocPostProcessor(DocPostProcessor docPostProcessor) {
        super.registerDocPostProcessor(new DocPostProcessorWrapper(docPostProcessor));
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
