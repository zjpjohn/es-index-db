package com.wxingyl.es.index.version;

import com.wxingyl.es.exception.IndexDocException;
import com.wxingyl.es.util.CommonUtils;
import com.wxingyl.es.util.EsUtils;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

/**
 * Created by xing on 15/9/14.
 * default index version manager
 */
public class DefaultIndexVersionManager implements IndexVersionManager {

    private IndicesAdminClient indicesAdminClient;

    private ThreadLocal<GetIndexRequest> getIndexRequest;

    public DefaultIndexVersionManager(Client client) {
        indicesAdminClient = client.admin().indices();
        getIndexRequest = CommonUtils.createThreadLocal(() -> {
            GetIndexRequest indexRequest = new GetIndexRequest();
            indexRequest.indices("_all");
            indexRequest.addFeatures(GetIndexRequest.Feature.MAPPINGS, GetIndexRequest.Feature.SETTINGS);
            return indexRequest;
        });
    }

    @Override
    public VersionIndexTypeBean topVersionIndex(final String indexName) {
        GetIndexResponse response = indicesAdminClient.getIndex(getIndexRequest.get()).actionGet();
        final String indexVersionStart = indexName + "_v";
        PriorityQueue<VersionIndex> queue = new PriorityQueue<>();
        for (String index : response.getIndices()) {
            if (index.equals(indexName)) {
                queue.add(new VersionIndex(0, indexName));
            } else if (index.startsWith(indexVersionStart)) {
                try {
                    int version = Integer.parseInt(index.substring(indexVersionStart.length()));
                    queue.add(new VersionIndex(version, indexName, index));
                } catch (NumberFormatException ignored) {
                }
            }
        }
        VersionIndex topVersion = queue.peek();
        if (topVersion != null) {
            topVersion.initConfig(response.mappings().get(topVersion.getVersionIndexName()),
                    response.settings().get(topVersion.getVersionIndexName()));
        }
        return new VersionIndexTypeBean(topVersion);
    }

    @Override
    public VersionIndex createNewIndex(String indexName) {
        VersionIndex nextVersion = new VersionIndex(1, indexName);
        indicesAdminClient.create(new CreateIndexRequest(nextVersion.getVersionIndexName()));
        return nextVersion;
    }

    @Override
    public VersionIndex createNextVersionIndex(VersionIndex curTopVersion) {
        VersionIndex nextVersion = new VersionIndex(curTopVersion.getVersion() + 1, curTopVersion.getIndexName());
        nextVersion.initConfig(curTopVersion.getMappings(), curTopVersion.getSettings());
        Map<String, String> typeMapping = new HashMap<>();
        nextVersion.getMappings().forEach(v -> {
            try {
                typeMapping.put(v.key, v.value.source().string());
            } catch (IOException e) {
                throw new IndexDocException("copy mapping failed, nextVersion: " + nextVersion, e);
            }
        });
        EsUtils.createNewIndex(indicesAdminClient, nextVersion.getVersionIndexName(), nextVersion.getSettings(), typeMapping);
        return nextVersion;
    }

    @Override
    public int supportMaxVersion() {
        return -1;
    }

    @Override
    public String supportIndex() {
        return null;
    }

}
