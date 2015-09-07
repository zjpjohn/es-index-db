package com.wxingyl.es.index;

import com.wxingyl.es.conf.index.IndexTypeBean;

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
    int indexTypeFill(IndexTypeBean typeBean);

    void registerDocPostProcessor(DocPostProcessor docPostProcessor);

    void registerBulkIndexGenerate(BulkIndexGenerate bulkIndexGenerate);

    void registerTableQueryResultListener(TableQueryResultListener listener);
}
