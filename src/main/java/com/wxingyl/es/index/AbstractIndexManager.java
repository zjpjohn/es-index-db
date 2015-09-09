package com.wxingyl.es.index;

import com.wxingyl.es.exception.IndexIllegalArgumentException;
import com.wxingyl.es.index.db.TableQueryResultHandle;
import com.wxingyl.es.index.post.DocPostProcessor;
import com.wxingyl.es.db.DbTableDesc;
import com.wxingyl.es.db.result.TableQueryResult;
import com.wxingyl.es.index.generator.BulkIndexGenerate;
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

    private Map<DbTableDesc, TableQueryResultHandle> tableQueryResultHandleMap = new HashMap<>();

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
    public void registerTableQueryResultHandle(TableQueryResultHandle handler) {
        if (CommonUtils.isEmpty(handler.supportTable())) {
            throw new IndexIllegalArgumentException("TableQueryResultListener: " + handler + " supportTable is empty");
        }
        handler.supportTable().forEach(table -> tableQueryResultHandleMap.put(table, handler));
    }

    protected DocPostProcessor getDocPostProcessor(IndexTypeDesc type) {
        return docPostProcessorMap.get(type);
    }

    protected BulkIndexGenerate getBulkIndexGenerator(IndexTypeDesc type) {
        return bulkIndexGeneratorMap.get(type);
    }

    protected void notifyTableQueryResultHandler(IndexTypeDesc type, List<TableQueryResult> queryResults) {
        queryResults.forEach(q -> {
            TableQueryResultHandle handler = tableQueryResultHandleMap.get(q.getBaseInfo().getTable());
            if (handler != null) {
                handler.onHandle(type, q);
            }
        });
    }

    protected Client getClient() {
        return client;
    }
}
