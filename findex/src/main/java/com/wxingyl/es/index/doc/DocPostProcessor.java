package com.wxingyl.es.index.doc;

import com.wxingyl.es.index.db.DbQueryDependResult;
import com.wxingyl.es.index.IndexTypeDesc;
import com.wxingyl.es.db.result.TableQueryResult;

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

    /**
     * create PageDocument by master table query result
     */
    PageDocument initMasterPageDoc(TableQueryResult masterResult);

    /**
     * if this slave-table is leaf table, don't have child slave-table, only self table query, will callback this function
     * @param masterPageDoc master PageDocument object
     * @param masterField master table key field
     * @param slaveResult leaf slave-table query result
     * @return return PageDocument, you can wrapper this PageDocument,
     *  if return null, it means don't handle, and will call defaultDocPostProcessor.applyTableQueryResult
     */
    PageDocument applyTableQueryResult(PageDocument masterPageDoc, String masterField, TableQueryResult slaveResult);

    /**
     * if a slave-table have child slave-table, not a leaf salve-table, need merge childPageDoc
     * @param masterPageDoc master PageDocument object
     * @param masterField   master table key field
     * @param childPageDoc child pageDocument
     * @return return PageDocument, you can wrapper this PageDocument,
     *  if return null, it means don't handle, and will call defaultDocPostProcessor.applyTableQueryResult
     */
    PageDocument mergeChildPageDoc(PageDocument masterPageDoc, String masterField, PageDocument childPageDoc);

    Set<IndexTypeDesc> supportType();
}
