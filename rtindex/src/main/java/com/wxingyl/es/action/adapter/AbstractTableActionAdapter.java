package com.wxingyl.es.action.adapter;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.wxingyl.es.action.TableAction;
import com.wxingyl.es.command.EntryPreQueryRtCommand;
import com.wxingyl.es.command.RtCommand;
import com.wxingyl.es.command.update.DefaultUpdateRtAction;
import com.wxingyl.es.command.update.UpdateFieldEntry;
import org.elasticsearch.index.query.QueryBuilders;

/**
 * Created by xing on 15/10/20.
 * abstract implement
 */
public abstract class AbstractTableActionAdapter implements TableActionAdapter {

    protected final IndexTypeInfo.TableInfo tableInfo;

    public AbstractTableActionAdapter(IndexTypeInfo.TableInfo tableInfo) {
        this.tableInfo = tableInfo;
        tableInfo.setActionAdapter(this);
    }


    /**
     * default updateRtCommand implement
     */
    @Override
    public RtCommand createUpdateRtCommand(CanalEntry.RowData rowData) {
        final int count = rowData.getAfterColumnsCount();
        EntryPreQueryRtCommand<UpdateFieldEntry> rtCommand = new DefaultUpdateRtAction(tableInfo);
        for (int i = 0; i < count; i++) {
            CanalEntry.Column afterColumn = rowData.getAfterColumns(i);
            if (afterColumn.getUpdated()) {
                rtCommand.addFieldEntry(tableInfo.getDocField(afterColumn.getName()), UpdateFieldEntry.build()
                        .beforeValue(rowData.getBeforeColumns(i).getValue())
                        .afterValue(afterColumn.getValue())
                        .isQueryCondition(true)
                        .build());
            }
        }
        rtCommand.addPreQuery(QueryBuilders.termQuery(tableInfo.getDocKeyField(),
                rowData.getBeforeColumns(tableInfo.getKeyFieldIndex()).getValue()));
        return rtCommand;
    }

    protected TableAction action() {
        return tableInfo.getTableAction();
    }

}
