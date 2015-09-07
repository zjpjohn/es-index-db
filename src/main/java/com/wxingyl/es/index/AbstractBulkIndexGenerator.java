package com.wxingyl.es.index;

import com.wxingyl.es.exception.IndexDocException;
import com.wxingyl.es.util.DateConvert;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.Client;

import java.io.IOException;

/**
 * Created by xing on 15/9/7.
 * abstract bulkIndexGenerator
 */
public abstract class AbstractBulkIndexGenerator implements BulkIndexGenerate {

    private DateConvert dateConvert;

    private int bulkRequestBatchSize = 5000;

    protected void setDateConvert(DateConvert dateConvert) {
        this.dateConvert = dateConvert;
    }

    protected void setBulkRequestBatchSize(int bulkRequestBatchSize) {
        this.bulkRequestBatchSize = bulkRequestBatchSize;
    }

    protected DateConvert getDateConvert() {
        return dateConvert;
    }

    protected int getBulkRequestBatchSize() {
        return bulkRequestBatchSize;
    }

    protected abstract String getDocId(DocumentBaseInfo baseInfo, DocFields doc);

    @Override
    public int bulkInsert(Client client, PageDocument pageDocument) {
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        int count = 0, ret = 0;
        DocumentBaseInfo baseInfo = pageDocument.getBaseInfo();
        IndexTypeDesc type = baseInfo.getType();
        for (DocFields doc : pageDocument) {
            try {
                bulkRequest.add(client.prepareIndex(type.getIndex(), type.getType(), getDocId(baseInfo, doc))
                        .setSource(doc.buildXContent(dateConvert)));
            } catch (IOException e) {
                throw new IndexDocException("create XContentBuilder error", e);
            }
            count++;
            if (count >= bulkRequestBatchSize) {
                bulkRequest.execute().actionGet();
                bulkRequest = client.prepareBulk();
                count = 0;
                ret += bulkRequest.numberOfActions();
            }
        }
        if (count > 0) {
            bulkRequest.execute().actionGet();
            ret += bulkRequest.numberOfActions();
        }
        return ret;
    }

}
