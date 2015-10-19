package com.wxingyl.es.action;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.wxingyl.es.command.RtCommand;
import com.wxingyl.es.command.UpdateRtCommand;
import com.wxingyl.es.command.UpdateRtCommandAction;
import com.wxingyl.es.db.DbTableDesc;
import com.wxingyl.es.index.IndexTypeDesc;
import com.wxingyl.es.util.transfer.StrValueConvert;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.List;
import java.util.Map;

/**
 * Created by xing on 15/10/19.
 * table default action
 */
public class DefaultTableAction extends AbstractTableAction {

    public DefaultTableAction(DbTableDesc table, Map<String, StrValueConvert> valueTransferMap) {
        super(table, valueTransferMap);
    }

    @Override
    protected void deleteCommand(IndexTypeDesc type, List<CanalEntry.Column> list, List<RtCommand> appendRet) {
        //TODO
    }

    @Override
    protected void insertCommand(IndexTypeDesc type, List<CanalEntry.Column> list, List<RtCommand> appendRet) {
        //TODO
    }

    @Override
    protected void updateCommand(IndexTypeDesc type, CanalEntry.RowData rowData, List<RtCommand> appendRet) {
        final int count = rowData.getAfterColumnsCount();
        IndexTypeInfo.TableInfo tableInfo = typeInfoMap.get(type);
        String keyField = tableInfo.getKeyField();
        UpdateRtCommand rtCommand = null;
        for (int i = 0; i < count; i++) {
            CanalEntry.Column afterColumn = rowData.getAfterColumns(i);
            if (afterColumn.getUpdated()) {
                if (keyField.equals(afterColumn.getName())) {
                    deleteCommand(type, rowData.getBeforeColumnsList(), appendRet);
                    insertCommand(type, rowData.getAfterColumnsList(), appendRet);
                    return;
                } else {
                    if (rtCommand == null) {
                        rtCommand = new UpdateRtCommandAction(tableInfo.getClient(), type, tableInfo.getIdField());
                    }
                    tableInfo.addChangedFieldEntry(afterColumn.getName(),
                            rowData.getBeforeColumns(i).getValue(), afterColumn.getValue(), rtCommand);
                }
            }
        }
        //can not run here
        if (rtCommand == null || rtCommand.isInvalid()) return;
        String keyFieldVal = rowData.getAfterColumns(tableColumnIndex.getIndex(keyField)).getValue();
        rtCommand.addPreQuery(QueryBuilders.termQuery(tableInfo.getDocField(keyField), valueConvertMap.get(keyField) == null
                ? keyFieldVal : valueConvertMap.get(keyField).convert(keyFieldVal)));
        appendRet.add(rtCommand);
    }
}
