package com.wxingyl.es.command.insert;

import com.wxingyl.es.action.IndexTypeInfo;
import com.wxingyl.es.command.AbstractRtCommand;
import com.wxingyl.es.db.result.TableQueryResult;
import com.wxingyl.es.index.doc.IndexDocTransfer;
import com.wxingyl.es.index.doc.PageDocument;

import java.util.List;
import java.util.Map;

/**
 * Created by xing on 15/10/21.
 * abstract MasterInsertRtCommand
 */
public abstract class AbstractMasterInsertRtCommand extends AbstractRtCommand implements MasterInsertRtCommand {

    public AbstractMasterInsertRtCommand(IndexTypeInfo.TableInfo tableInfo) {
        super(tableInfo);
    }

    protected abstract List<Map<String, Object>> getTableResultData();

    @Override
    public PageDocument docCreate() {
        if (isInvalid()) return null;
        IndexDocTransfer transfer = tableInfo.getIndexManager().getIndexDocTransfer();
        PageDocument document = transfer.indexDocCreate(tableInfo.getTypeBean(),
                TableQueryResult.build()
                        .dbData(getTableResultData())
                        .build(tableInfo.getSqlQueryInfo()));
        return document == null ? null : document;
    }

    @Override
    public IndexTypeInfo.TableInfo getTableInfo() {
        return tableInfo;
    }
}
