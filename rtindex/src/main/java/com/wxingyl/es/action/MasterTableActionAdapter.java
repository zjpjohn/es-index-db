package com.wxingyl.es.action;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.wxingyl.es.command.delete.DeleteRtCommand;
import com.wxingyl.es.command.delete.SingleMasterDeleteRtCommandAction;
import com.wxingyl.es.command.insert.InsertRtCommand;
import com.wxingyl.es.command.insert.SingleMasterInsertRtCommandAction;

import java.util.List;

/**
 * Created by xing on 15/10/20.
 * default implement of TypeTableActionAdapter
 */
public class MasterTableActionAdapter extends AbstractTypeTableActionAdapter {

    public MasterTableActionAdapter(IndexTypeInfo.TableInfo tableInfo) {
        super(tableInfo);
        if (!tableInfo.isMasterTable()) {
            throw new IllegalStateException("table: " + tableInfo.getTable() + " is not master table");
        }
    }

    @Override
    public InsertRtCommand createInsertRtCommand(List<CanalEntry.Column> list) {
        return new SingleMasterInsertRtCommandAction(tableInfo, tableInfo.getTableAction().canalRowTransfer(list));
    }

    @Override
    public DeleteRtCommand createDeleteRtCommand(List<CanalEntry.Column> list) {
        return new SingleMasterDeleteRtCommandAction(tableInfo, list.get(tableInfo.getKeyFieldIndex()).getValue());
    }

}
