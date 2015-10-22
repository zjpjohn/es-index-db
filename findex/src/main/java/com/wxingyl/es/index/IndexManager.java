package com.wxingyl.es.index;

import com.wxingyl.es.conf.ConfigManager;
import com.wxingyl.es.index.doc.IndexDocTransfer;
import com.wxingyl.es.index.generator.BulkIndexGenerate;
import com.wxingyl.es.index.version.IndexVersionManager;
import com.wxingyl.es.util.Listener;
import org.elasticsearch.client.Client;

import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 * Created by xing on 15/10/8.
 * indexManager interface
 */
public interface IndexManager {

    void setExecutorService(ExecutorService executorService);

    void switchDefaultIndexVersionManager(boolean turnOn);

    void registerBulkIndexGenerate(BulkIndexGenerate bulkIndexGenerate);

    void registerIndexVersionManager(IndexVersionManager indexVersionManager);

    void registerIndexEventListener(Listener<IndexTypeEvent> listener);

    Client getClient();

    ConfigManager getConfigManager();

    IndexDocTransfer getIndexDocTransfer();

    BulkIndexGenerate getBulkIndexGenerate(IndexTypeDesc type);

    long indexFill(String index, String type);

    Map<String, Long> indexFill(String index);

    /**
     * @param index         index name
     * @param type          if type == null, it mean create all type below the index
     * @param concurrentNum concurrent thread num, if num > 1, executorService must not null
     * @return Map, key: type, value: document total num
     */
    Map<String, Long> indexFill(String index, String type, int concurrentNum);

}
