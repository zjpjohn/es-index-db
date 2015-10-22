package com.wxingyl.es.index.generator;

import com.wxingyl.es.index.IndexTypeDesc;
import com.wxingyl.es.index.doc.PageDocument;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;

import java.util.List;
import java.util.Set;

/**
 * Created by xing on 15/9/7.
 * generate index to elastic search server
 */
public interface BulkIndexGenerate {

    int bulkInsert(Client client, PageDocument pageDocument);

    List<IndexRequestBuilder> buildIndexRequest(Client client, PageDocument pageDocument);

    Set<IndexTypeDesc> supportType();
}
