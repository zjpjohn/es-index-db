package com.wxingyl.es.action.adapter;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.wxingyl.es.command.delete.DeleteRtCommand;
import com.wxingyl.es.command.delete.MasterDeleteRtCommandAction;
import com.wxingyl.es.command.insert.InsertRtCommand;
import com.wxingyl.es.command.insert.SingleMasterInsertRtCommandAction;

import java.util.List;

/**
 * Created by xing on 15/10/20.
 * default implement to master table of type
 */
public class MasterTableActionAdapter extends AbstractTableActionAdapter {

    public MasterTableActionAdapter(IndexTypeInfo.TableInfo tableInfo) {
        super(tableInfo);
        if (!tableInfo.isMasterTable()) {
            throw new IllegalStateException("table: " + tableInfo.getTable() + " in type: " +
                    tableInfo.getType() + " is not master table");
        }
    }

    @Override
    public InsertRtCommand createInsertRtCommand(List<CanalEntry.Column> list) {
        return new SingleMasterInsertRtCommandAction(tableInfo, tableInfo.getTableAction().canalRowTransfer(list));
    }

    @Override
    public DeleteRtCommand createDeleteRtCommand(List<CanalEntry.Column> list) {
        return new MasterDeleteRtCommandAction(tableInfo, list.get(tableInfo.getKeyFieldIndex()).getValue());
    }

}
