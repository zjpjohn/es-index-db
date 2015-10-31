package com.wxingyl.es.action.adapter;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.wxingyl.es.command.RtCommand;
import com.wxingyl.es.command.delete.MasterDeleteRtActionAction;
import com.wxingyl.es.command.insert.SingleMasterInsertRtAction;

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
    public RtCommand createInsertRtCommand(List<CanalEntry.Column> list) {
        return new SingleMasterInsertRtAction(tableInfo, tableInfo.getTableAction().canalRowTransfer(list));
    }

    @Override
    public RtCommand createDeleteRtCommand(List<CanalEntry.Column> list) {
        return new MasterDeleteRtActionAction(tableInfo, list.get(tableInfo.getKeyFieldIndex()).getValue());
    }

}
