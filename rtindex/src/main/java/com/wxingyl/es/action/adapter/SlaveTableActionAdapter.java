package com.wxingyl.es.action.adapter;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.wxingyl.es.command.delete.DeleteRtCommand;
import com.wxingyl.es.command.insert.InsertRtCommand;

import java.util.List;

/**
 * Created by xing on 15/10/23.
 * default implement to slave table of type
 */
public class SlaveTableActionAdapter extends AbstractTableActionAdapter {

    public SlaveTableActionAdapter(IndexTypeInfo.TableInfo tableInfo) {
        super(tableInfo);
        if (tableInfo.isMasterTable()) {
            throw new IllegalStateException("table: " + tableInfo.getTable() + " in type: " +
                    tableInfo.getType() + " is master table");
        }
    }

    @Override
    public InsertRtCommand createInsertRtCommand(List<CanalEntry.Column> list) {
        return null;
    }

    @Override
    public DeleteRtCommand createDeleteRtCommand(List<CanalEntry.Column> list) {
        return null;
    }
}
