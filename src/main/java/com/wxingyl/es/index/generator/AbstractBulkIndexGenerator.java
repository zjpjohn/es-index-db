package com.wxingyl.es.index.generator;

import com.wxingyl.es.exception.IndexDocException;
import com.wxingyl.es.index.IndexTypeDesc;
import com.wxingyl.es.index.post.DocFields;
import com.wxingyl.es.index.TypeBaseInfo;
import com.wxingyl.es.index.post.PageDocument;
import com.wxingyl.es.util.DateConvert;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;

import java.io.IOException;

/**
 * Created by xing on 15/9/7.
 * abstract bulkIndexGenerator
 */
public abstract class AbstractBulkIndexGenerator implements BulkIndexGenerate {

    protected static final int DEFAULT_BULK_BATCH_SIZE = 2000;

    private DateConvert dateConvert;

    private int bulkRequestBatchSize = DEFAULT_BULK_BATCH_SIZE;

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

    protected abstract String getDocId(TypeBaseInfo baseInfo, DocFields doc);

    @Override
    public int bulkInsert(Client client, PageDocument pageDocument) {
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        int count = 0, ret = 0;
        TypeBaseInfo baseInfo = pageDocument.getBaseInfo();
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
                ret += executeIndexRequest(bulkRequest);
                bulkRequest = client.prepareBulk();
                count = 0;
            }
        }
        if (count > 0) {
            ret += executeIndexRequest(bulkRequest);
        }
        return ret;
    }

    /**
     * @return return succeed request num
     */
    protected int executeIndexRequest(BulkRequestBuilder bulkRequest) {
        BulkResponse response = bulkRequest.execute().actionGet();
        if (response.hasFailures()) {
            return bulkRequest.numberOfActions() - handleFailed(bulkRequest, response);
        } else {
            return bulkRequest.numberOfActions();
        }
    }

    /**
     * @return return failed number
     */
    protected abstract int handleFailed(BulkRequestBuilder bulkRequest, BulkResponse response);

}
