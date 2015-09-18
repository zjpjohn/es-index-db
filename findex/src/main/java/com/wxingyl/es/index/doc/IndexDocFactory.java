package com.wxingyl.es.index.doc;

import com.wxingyl.es.index.IndexTypeBean;
import com.wxingyl.es.index.db.TableQueryResultHandle;

/**
 * Created by xing on 15/9/7.
 * index manager
 */
public interface IndexDocFactory {

    /**
     * index type fill document for type
     * @param typeBean type
     * @param startPage start page to create index, from 0 to count
     * @param endPage end page to quit create index, if endPage <= 0, it mean not limit
     * @return create document iterator
     */
    PageDocumentIterator indexDocCreate(IndexTypeBean typeBean, int startPage, int endPage);

    void registerDocPostProcessor(DocPostProcessor docPostProcessor);

    void registerTableQueryResultHandle(TableQueryResultHandle listener);
}
