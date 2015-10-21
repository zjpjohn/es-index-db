package com.wxingyl.es.action;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.wxingyl.es.command.insert.InsertRtCommand;
import com.wxingyl.es.command.insert.SingleMasterInsertRtCommandAction;
import com.wxingyl.es.command.update.ChangedFieldEntry;
import com.wxingyl.es.command.update.UpdateRtCommand;
import com.wxingyl.es.command.update.UpdateRtCommandAction;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by xing on 15/10/20.
 * default implement of TypeTableActionAdapter
 */
public class DefaultTypeTableActionAdapter extends AbstractTypeTableActionAdapter {

    private Map<String, String> dbDocFieldMap;

    public DefaultTypeTableActionAdapter(IndexTypeInfo.TableInfo tableInfo, Map<String, String> dbDocFieldMap) {
        super(tableInfo);
        this.dbDocFieldMap = new HashMap<>();
        this.dbDocFieldMap.putAll(dbDocFieldMap);
    }

    @Override
    public InsertRtCommand createInsertRtCommand(List<CanalEntry.Column> list) {
        if (tableInfo.isMasterTable()) {
            return new SingleMasterInsertRtCommandAction(tableInfo, tableInfo.getTableAction().canalRowTransfer(list));
        }
        //TODO other insert need implement
        return null;
    }

    @Override
    public UpdateRtCommand createUpdateRtCommand(CanalEntry.RowData rowData) {
        final int count = rowData.getAfterColumnsCount();
        UpdateRtCommand rtCommand = new UpdateRtCommandAction(tableInfo);
        for (int i = 0; i < count; i++) {
            CanalEntry.Column afterColumn = rowData.getAfterColumns(i);
            if (afterColumn.getUpdated()) {
                rtCommand.addChangeField(new ChangedFieldEntry(dbDocFieldMap.get(afterColumn.getName()),
                        rowData.getBeforeColumns(i).getValue(), afterColumn.getValue()));
            }
        }
        String keyField = tableInfo.getKeyField();
        rtCommand.addPreQuery(QueryBuilders.termQuery(dbDocFieldMap.get(keyField),
                rowData.getBeforeColumns(tableInfo.getTableAction().getColumnIndex(keyField)).getValue()));
        return rtCommand;
    }
}
