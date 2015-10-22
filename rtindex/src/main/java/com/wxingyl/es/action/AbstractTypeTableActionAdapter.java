package com.wxingyl.es.action;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.wxingyl.es.command.update.ChangedFieldEntry;
import com.wxingyl.es.command.update.UpdateRtCommand;
import com.wxingyl.es.command.update.UpdateRtCommandAction;
import org.elasticsearch.index.query.QueryBuilders;

/**
 * Created by xing on 15/10/20.
 * abstract implement
 */
public abstract class AbstractTypeTableActionAdapter implements TypeTableActionAdapter {

    protected final IndexTypeInfo.TableInfo tableInfo;

    public AbstractTypeTableActionAdapter(IndexTypeInfo.TableInfo tableInfo) {
        this.tableInfo = tableInfo;
        tableInfo.setActionAdapter(this);
    }


    /**
     * default updateRtCommand implement
     */
    @Override
    public UpdateRtCommand createUpdateRtCommand(CanalEntry.RowData rowData) {
        final int count = rowData.getAfterColumnsCount();
        UpdateRtCommand rtCommand = new UpdateRtCommandAction(tableInfo);
        for (int i = 0; i < count; i++) {
            CanalEntry.Column afterColumn = rowData.getAfterColumns(i);
            if (afterColumn.getUpdated()) {
                rtCommand.addChangeField(new ChangedFieldEntry(tableInfo.getDocField(afterColumn.getName()),
                        rowData.getBeforeColumns(i).getValue(), afterColumn.getValue()));
            }
        }
        rtCommand.addPreQuery(QueryBuilders.termQuery(tableInfo.getDocKeyField(),
                rowData.getBeforeColumns(tableInfo.getKeyFieldIndex()).getValue()));
        return rtCommand;
    }
}
