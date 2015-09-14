package com.wxingyl.es.index.version;

import org.elasticsearch.client.Client;

import java.util.Set;

/**
 * Created by xing on 15/9/14.
 * default index version manager
 */
public class DefaultIndexVersionManager implements IndexVersionManager {

    private Client client;

    public DefaultIndexVersionManager(Client client) {
        this.client = client;
    }

    @Override
    public VersionIndexTypeBean getIndexVersion(String indexName) {
        return null;
    }

    @Override
    public Set<String> supportIndex() {
        return null;
    }

}
