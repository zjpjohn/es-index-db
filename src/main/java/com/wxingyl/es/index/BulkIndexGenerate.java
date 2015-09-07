package com.wxingyl.es.index;

import org.elasticsearch.client.Client;

import java.util.Set;

/**
 * Created by xing on 15/9/7.
 * generate index to elastic search server
 */
public interface BulkIndexGenerate {

    int bulkInsert(Client client, PageDocument pageDocument);

    Set<IndexTypeDesc> supportType();
}
