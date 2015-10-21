package com.wxingyl.es.index.doc;

import com.wxingyl.es.db.result.TableQueryResult;
import com.wxingyl.es.index.IndexTypeBean;
import com.wxingyl.es.index.db.DbQueryDependResult;
import com.wxingyl.es.index.db.TableQueryResultHandle;

/**
 * Created by xing on 15/9/7.
 * index document factory, transfer db query result to document
 */
public interface IndexDocTransfer {

    /**
     * index type fill document for type
     * @param typeBean type
     * @param startPage start page to create index, from 0 to count
     * @param endPage end page to quit create index, if endPage <= 0, it mean not limit
     * @return create document iterator
     */
    PageDocumentIterator indexDocCreate(IndexTypeBean typeBean, int startPage, int endPage);

    PageDocument indexDocCreate(IndexTypeBean typeBean, TableQueryResult queryResult);

    void registerDocPostProcessor(DocPostProcessor docPostProcessor);

    void registerTableQueryResultHandle(TableQueryResultHandle listener);
}
