package com.wxingyl.es.index;

import com.wxingyl.es.jdal.TableQueryResult;

/**
 * Created by xing on 15/9/6.
 * Factory hook that allows for custom modification of create index document
 */
public interface DocPostProcessor<T extends DocFields> {
    /**
     * return null, will post by default
     */
    PageDocument<T> postProcessor(DbQueryDependResult masterResult);

    PageDocument<T> initMasterPageDoc(TableQueryResult masterResult);

    <R extends DocFields> PageDocument<R> applyTableQueryResult(PageDocument<R> masterPageDoc, String masterField,
                                                                TableQueryResult slaveResult);

    <R extends DocFields> PageDocument<R> mergeChildPageDoc(PageDocument<R> masterPageDoc, String masterField,
                                                            PageDocument<R> childPageDoc);

    IndexTypeDesc supportType();
}
