package com.wxingyl.es.util;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.settings.Settings;

import java.util.Map;

/**
 * Created by xing on 15/9/14.
 * Es utils
 */
public abstract class EsUtils {

    public static void createNewIndex(IndicesAdminClient indicesAdminClient, String index,
                                      Settings settings, Map<String, String> typeMapping) {
        indicesAdminClient.create(new CreateIndexRequest(index, settings));
        if (CommonUtils.isEmpty(typeMapping)) return;
        PutMappingRequest mappingRequest = Requests.putMappingRequest(index);
        typeMapping.forEach((k, v) -> {
            mappingRequest.type(k);
            mappingRequest.source(v);
            indicesAdminClient.putMapping(mappingRequest);
        });
    }
}
