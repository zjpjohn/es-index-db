package com.wxingyl.es.index;

import com.wxingyl.es.jdal.TableQueryResult;

import java.util.Set;

/**
 * Created by xing on 15/9/6.
 * Factory hook that allows for custom modification of create index document
 */
public interface DocPostProcessor {

    /**
     * @param type now argument is type, will may add more, define a bean class
     */
    void startPost(IndexTypeDesc type);

    void endPost(IndexTypeDesc type);

    /**
     * return null, will post by default
     */
    PageDocument postProcessor(DbQueryDependResult masterResult);

    PageDocument initMasterPageDoc(TableQueryResult masterResult);

    PageDocument applyTableQueryResult(PageDocument masterPageDoc, String masterField, TableQueryResult slaveResult);

    PageDocument mergeChildPageDoc(PageDocument masterPageDoc, String masterField, PageDocument childPageDoc);

    Set<IndexTypeDesc> supportType();
}
