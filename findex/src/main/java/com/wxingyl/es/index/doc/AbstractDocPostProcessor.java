package com.wxingyl.es.index.doc;

import com.wxingyl.es.index.TypeBaseInfo;
import com.wxingyl.es.index.db.DbQueryDependResult;
import com.wxingyl.es.index.IndexTypeDesc;
import com.wxingyl.es.db.result.TableQueryResult;

/**
 * Created by xing on 15/9/7.
 * abstract DocPostProcessor
 */
public abstract class AbstractDocPostProcessor implements DocPostProcessor {

    private ThreadLocal<IndexTypeDesc> postEvn = new ThreadLocal<>();

    @Override
    public PageDocument postProcessor(DbQueryDependResult masterResult) {
        return null;
    }

    @Override
    public PageDocument initMasterPageDoc(TableQueryResult masterResult) {
        PageDocument masterPageDoc = new PageDocument(TypeBaseInfo.build(masterResult.getBaseInfo(), getPostEvn()));
        masterPageDoc.addAll(DocFields.build(masterResult.getDbData()));
        return masterPageDoc;
    }

    @Override
    public void startPost(IndexTypeDesc type) {
        postEvn.set(type);
    }

    @Override
    public void endPost(IndexTypeDesc type) {
        postEvn.remove();
    }

    protected IndexTypeDesc getPostEvn() {
        return postEvn.get();
    }
}
