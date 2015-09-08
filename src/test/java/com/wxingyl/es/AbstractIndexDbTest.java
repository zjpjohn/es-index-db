package com.wxingyl.es;

import com.wxingyl.es.conf.ConfigManager;
import com.wxingyl.es.conf.DefaultConfigManager;
import com.wxingyl.es.index.DefaultIndexManager;
import com.wxingyl.es.index.IndexManager;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.junit.Before;
import org.junit.BeforeClass;

/**
 * Created by xing on 15/9/8.
 * index db test
 */
public abstract class AbstractIndexDbTest {

    protected static TransportClient client;

    protected static ConfigManager configManager;

    protected static IndexManager indexManager;

    @BeforeClass
    public static void setup() {
        client = new TransportClient(ImmutableSettings.settingsBuilder()
                .put("client.transport.sniff", true) // sniff the rest of cluster so we only need to set one ip
                .put("cluster.name", "xing-elasticsearch")
                .put("client.transport.ping_timeout", "20s")
                .put("client.transport.nodes_sampler_interval", "20s")
                .build());
        client.addTransportAddress(new InetSocketTransportAddress("127.0.0.1", 9300));
        indexManager = new DefaultIndexManager(client);
        configManager = new DefaultConfigManager();
        System.out.println("~~~~~~~~~ setup end ~~~~~~~~~~~");
    }

    @Before
    public void initConfig() {
        configManager.parseDataSource("/Users/xing/code/db-river-elasticsearch/src/test/resources/datasource.yml");
        configManager.parseIndexType("/Users/xing/code/db-river-elasticsearch/src/test/resources/index_data.yml");
        System.out.println("~~~~~~~~~ init config end ~~~~~~~~~~~");
    }
}
