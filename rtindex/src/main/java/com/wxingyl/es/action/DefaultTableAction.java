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
        //TODO delete command
    }

    @Override
    protected void insertCommand(IndexTypeDesc type, List<CanalEntry.Column> list, List<RtCommand> appendRet) {
        //TODO insert command
    }

    @Override
    protected void updateCommand(IndexTypeDesc type, CanalEntry.RowData rowData, List<RtCommand> appendRet) {
        IndexTypeInfo.TableInfo tableInfo = typeInfoMap.get(type);
        String keyField = tableInfo.getKeyField();
        CanalEntry.Column keyColumn = rowData.getAfterColumns(tableColumnIndex.getIndex(keyField));
        if (keyColumn.getUpdated()) {
            deleteCommand(type, rowData.getBeforeColumnsList(), appendRet);
            insertCommand(type, rowData.getAfterColumnsList(), appendRet);
            return;
        }
        RtCommand command = tableInfo.getTypeActionAdapt().createUpdateRtCommand(rowData);
        if (command != null) appendRet.add(command);
    }
}
