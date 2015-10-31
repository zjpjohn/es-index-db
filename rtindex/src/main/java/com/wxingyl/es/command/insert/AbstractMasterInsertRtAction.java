package com.wxingyl.es.command.insert;

import com.wxingyl.es.action.adapter.IndexTypeInfo;
import com.wxingyl.es.command.AbstractRtAction;
import com.wxingyl.es.command.MasterRtCommand;
import com.wxingyl.es.db.result.TableQueryResult;
import com.wxingyl.es.index.doc.IndexDocTransfer;
import com.wxingyl.es.index.doc.PageDocument;
import com.wxingyl.es.index.generator.BulkIndexGenerate;
import com.wxingyl.es.util.CommonUtils;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;

import java.util.List;
import java.util.Map;

/**
 * Created by xing on 15/10/21.
 * abstract MasterInsertRtCommand
 */
public abstract class AbstractMasterInsertRtAction extends AbstractRtAction implements MasterRtCommand {

    public AbstractMasterInsertRtAction(IndexTypeInfo.TableInfo tableInfo) {
        super(tableInfo);
    }

    protected abstract List<Map<String, Object>> getTableResultData();

    public abstract boolean isInvalid();

    @Override
    public ActionRequestBuilder makeRequest() {
        IndexDocTransfer transfer = tableInfo.getIndexManager().getIndexDocTransfer();
        PageDocument document = transfer.indexDocCreate(tableInfo.getTypeBean(),
                TableQueryResult.build()
                        .dbData(getTableResultData())
                        .build(tableInfo.getSqlQueryInfo()));
        if (document == null || document.size() == 0) return null;
        BulkIndexGenerate bulkIndexGenerate = tableInfo.getIndexManager().getBulkIndexGenerate(tableInfo.getType());
        List<IndexRequestBuilder> list = bulkIndexGenerate.buildIndexRequest(getClient(), document);
        if (CommonUtils.isEmpty(list)) return null;
        else if (list.size() == 1) return list.get(0);
        else {
            BulkRequestBuilder bulkRequest = getClient().prepareBulk();
            for (IndexRequestBuilder b : list) {
                bulkRequest.add(b);
            }
            return bulkRequest;
        }
    }

    @Override
    public IndexTypeInfo.TableInfo getTableInfo() {
        return tableInfo;
    }
}
