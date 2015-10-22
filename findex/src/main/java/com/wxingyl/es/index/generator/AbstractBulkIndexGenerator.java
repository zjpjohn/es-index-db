package com.wxingyl.es.index.generator;

import com.wxingyl.es.exception.IndexDocException;
import com.wxingyl.es.index.TypeBaseInfo;
import com.wxingyl.es.index.doc.DocFields;
import com.wxingyl.es.index.doc.PageDocument;
import com.wxingyl.es.util.DateConvert;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by xing on 15/9/7.
 * abstract bulkIndexGenerator
 */
public abstract class AbstractBulkIndexGenerator implements BulkIndexGenerate {

    protected final DateConvert dateConvert;

    public AbstractBulkIndexGenerator(DateConvert dateConvert) {
        this.dateConvert = dateConvert;
    }

    protected abstract String getDocId(TypeBaseInfo baseInfo, DocFields doc);

    @Override
    public List<IndexRequestBuilder> buildIndexRequest(Client client, PageDocument pageDocument) {
        final TypeBaseInfo baseInfo = pageDocument.getBaseInfo();
        final String index = baseInfo.getType().getIndex();
        final String type = baseInfo.getType().getType();
        List<IndexRequestBuilder> retList = new ArrayList<>(pageDocument.size());
        for (DocFields doc : pageDocument) {
            try {
                retList.add(client.prepareIndex(index, type, getDocId(baseInfo, doc))
                        .setSource(doc.buildXContent(dateConvert)));
            } catch (IOException e) {
                throw new IndexDocException("create XContentBuilder error", e);
            }
        }
        return retList;
    }

    @Override
    public int bulkInsert(Client client, PageDocument pageDocument) {
        if (pageDocument.size() == 0) return 0;
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        for (IndexRequestBuilder b : buildIndexRequest(client, pageDocument)) {
            bulkRequest.add(b);
        }
        return executeIndexRequest(bulkRequest);
    }

    /**
     * @return return succeed request num
     */
    private int executeIndexRequest(BulkRequestBuilder bulkRequest) {
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
