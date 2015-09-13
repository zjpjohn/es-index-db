package com.wxingyl.es.index.doc;

import com.wxingyl.es.index.IndexTypeBean;
import com.wxingyl.es.index.db.TableQueryResultHandle;
import com.wxingyl.es.index.generator.BulkIndexGenerate;

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
    PageDocumentIterator indexDocCreate(IndexTypeBean typeBean);

    void registerDocPostProcessor(DocPostProcessor docPostProcessor);

    void registerBulkIndexGenerate(BulkIndexGenerate bulkIndexGenerate);

    void registerTableQueryResultHandle(TableQueryResultHandle listener);
}
