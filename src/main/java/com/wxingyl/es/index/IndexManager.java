package com.wxingyl.es.index;

import com.wxingyl.es.conf.index.IndexTypeBean;
import com.wxingyl.es.index.db.TableQueryResultHandle;
import com.wxingyl.es.index.post.DocPostProcessor;
import com.wxingyl.es.index.generator.BulkIndexGenerate;

/**
 * Created by xing on 15/9/7.
 * index manager
 */
public interface IndexManager {

    /**
     * index type fill document for type
     * @param typeBean type
     * @return create document number
     */
    long indexTypeFill(IndexTypeBean typeBean);

    void registerDocPostProcessor(DocPostProcessor docPostProcessor);

    void registerBulkIndexGenerate(BulkIndexGenerate bulkIndexGenerate);

    void registerTableQueryResultHandle(TableQueryResultHandle listener);
}
