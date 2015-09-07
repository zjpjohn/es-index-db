package com.wxingyl.es.index;

import com.wxingyl.es.util.DefaultDateConvert;

import java.util.Set;

/**
 * Created by xing on 15/9/7.
 * default BulkIndexGenerate
 */
public class DefaultBulkIndexGenerator extends AbstractBulkIndexGenerator {

    public DefaultBulkIndexGenerator() {
        super();
        setDateConvert(DefaultDateConvert.INSTANCE);
        setBulkRequestBatchSize(5000);
    }

    @Override
    public Set<IndexTypeDesc> supportType() {
        return null;
    }

    @Override
    protected String getDocId(DocumentBaseInfo baseInfo, DocFields doc) {
        return doc.get(baseInfo.getKeyField()).toString();
    }
}
