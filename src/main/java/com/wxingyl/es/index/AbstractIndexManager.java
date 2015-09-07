package com.wxingyl.es.index;

import com.wxingyl.es.exception.IndexIllegalArgumentException;
import com.wxingyl.es.jdal.DbTableDesc;
import com.wxingyl.es.jdal.TableQueryResult;
import com.wxingyl.es.util.CommonUtils;
import org.elasticsearch.client.Client;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by xing on 15/9/7.
 * AbstractIndexManager
 * define some register function
 */
public abstract class AbstractIndexManager implements IndexManager {

    private Client client;

    private Map<IndexTypeDesc, DocPostProcessor> docPostProcessorMap = new HashMap<>();

    private Map<IndexTypeDesc, BulkIndexGenerate> bulkIndexGeneratorMap = new HashMap<>();

    private Map<DbTableDesc, TableQueryResultListener> tableQueryResultListenerMap = new HashMap<>();

    public AbstractIndexManager(Client client) {
        this.client = client;
    }

    @Override
    public void registerDocPostProcessor(DocPostProcessor docPostProcessor) {
        if (CommonUtils.isEmpty(docPostProcessor.supportType())) {
            throw new IndexIllegalArgumentException("DocPostProcessor: " + docPostProcessor + " supportType is empty");
        }
        docPostProcessor.supportType().forEach(v -> docPostProcessorMap.put(v, docPostProcessor));
    }

    @Override
    public void registerBulkIndexGenerate(BulkIndexGenerate bulkIndexGenerate) {
        if (CommonUtils.isEmpty(bulkIndexGenerate.supportType())) {
            throw new IndexIllegalArgumentException("BulkIndexGenerate: " + bulkIndexGenerate + " supportType is empty");
        }
        bulkIndexGenerate.supportType().forEach(v -> bulkIndexGeneratorMap.put(v, bulkIndexGenerate));
    }

    @Override
    public void registerTableQueryResultListener(TableQueryResultListener listener) {
        if (CommonUtils.isEmpty(listener.supportTable())) {
            throw new IndexIllegalArgumentException("TableQueryResultListener: " + listener + " supportTable is empty");
        }
        listener.supportTable().forEach(table -> tableQueryResultListenerMap.put(table, listener));
    }

    protected DocPostProcessor getDocPostProcessor(IndexTypeDesc type) {
        return docPostProcessorMap.get(type);
    }

    protected BulkIndexGenerate getBulkIndexGenerator(IndexTypeDesc type) {
        return bulkIndexGeneratorMap.get(type);
    }

    protected void notifyTableQueryResultListener(IndexTypeDesc type, List<TableQueryResult> queryResults) {
        queryResults.forEach(q -> {
            TableQueryResultListener listener = tableQueryResultListenerMap.get(q.getTable());
            if (listener != null) {
                listener.onHandle(type, q);
            }
        });
    }

    protected Client getClient() {
        return client;
    }
}
