package com.wxingyl.es.index;

import com.wxingyl.es.conf.index.IndexTypeBean;
import com.wxingyl.es.index.doc.DefaultDocPostProcessor;
import com.wxingyl.es.index.doc.DocPostProcessor;
import com.wxingyl.es.index.doc.PageDocument;
import com.wxingyl.es.jdal.TableQueryResult;
import org.elasticsearch.client.Client;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Created by xing on 15/9/6.
 * should singleton obj
 */
public class DefaultIndexManager extends AbstractIndexManager {

    private DocPostProcessor defaultDocPostProcessor;

    private BulkIndexGenerate defaultBulkIndexGenerator;

    public DefaultIndexManager(Client client) {
        super(client);
        defaultDocPostProcessor = new DefaultDocPostProcessor();
        defaultBulkIndexGenerator = new DefaultBulkIndexGenerator();
    }

    private PageDocument document(DocPostProcessor docPostProcessor,
                                  PageDocument pageDocument, DbQueryDependResult queryResult) {
        for (Map.Entry<String, DbQueryDependResult> e : queryResult.getSlaveResult().entries()) {
            DbQueryDependResult result = e.getValue();
            if (pageDocument == null) {
                pageDocument = docPostProcessor == null ? defaultDocPostProcessor.initMasterPageDoc(queryResult.getTableQueryResult())
                        : docPostProcessor.initMasterPageDoc(queryResult.getTableQueryResult());
            }
            Objects.requireNonNull(pageDocument);
            String masterField = e.getKey();
            if (result.getSlaveResult() == null) {
                pageDocument = docPostProcessor == null ? defaultDocPostProcessor.applyTableQueryResult(pageDocument, masterField, result.getTableQueryResult())
                        : docPostProcessor.applyTableQueryResult(pageDocument, masterField, result.getTableQueryResult());
            } else {
                PageDocument childDocument = document(docPostProcessor, null, result);
                if (childDocument != null) {
                    pageDocument = docPostProcessor == null ? defaultDocPostProcessor.mergeChildPageDoc(pageDocument, masterField, childDocument)
                            : docPostProcessor.mergeChildPageDoc(pageDocument, masterField, childDocument);
                }
            }
        }
        return pageDocument;
    }

    @Override
    public int indexTypeFill(IndexTypeBean typeBean) {
        IndexTypeDesc type = typeBean.getType();
        TableDependQuery query = new TableDependQuery(typeBean.getMasterTable());

        BulkIndexGenerate bulkIndexGenerate = getBulkIndexGenerator(type);
        if (bulkIndexGenerate == null) bulkIndexGenerate = defaultBulkIndexGenerator;

        DocPostProcessor docPostProcessor = getDocPostProcessor(type);
        if (docPostProcessor == null) {
            defaultDocPostProcessor.startPost(type);
        } else {
            docPostProcessor.startPost(type);
        }

        int docCount = 0;
        while (query.hasNext()) {
            DbQueryDependResult ret = query.next();
            notifyTableQueryResultListener(type, ret.getAllTableResult());

            PageDocument pageDocument = null;
            if (docPostProcessor != null) {
                pageDocument = docPostProcessor.postProcessor(ret);
            }
            if (pageDocument == null) {
                if (ret.getSlaveResult() == null) {
                    pageDocument = docPostProcessor == null ? defaultDocPostProcessor.initMasterPageDoc(ret.getTableQueryResult())
                            : docPostProcessor.initMasterPageDoc(ret.getTableQueryResult());
                } else {
                    pageDocument = document(docPostProcessor, null, ret);
                }
            }
            if (pageDocument != null) {
                docCount += bulkIndexGenerate.bulkInsert(getClient(), pageDocument);
            }
        }

        if (docPostProcessor == null) {
            defaultDocPostProcessor.endPost(type);
        } else {
            docPostProcessor.endPost(type);
        }
        return docCount;
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
