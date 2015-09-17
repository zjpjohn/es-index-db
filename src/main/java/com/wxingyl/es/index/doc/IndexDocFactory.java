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
     * @return create document iterator
     */
    PageDocumentIterator indexDocCreate(IndexTypeBean typeBean, int startPage);

    void registerDocPostProcessor(DocPostProcessor docPostProcessor);

    void registerTableQueryResultHandle(TableQueryResultHandle listener);
}
