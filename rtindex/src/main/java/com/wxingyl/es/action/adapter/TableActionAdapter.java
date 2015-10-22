package com.wxingyl.es.action.adapter;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.wxingyl.es.command.RtCommand;
import com.wxingyl.es.command.delete.DeleteRtCommand;
import com.wxingyl.es.command.insert.InsertRtCommand;
import com.wxingyl.es.command.update.UpdateRtCommand;

import java.util.List;

/**
 * Created by xing on 15/10/20.
 * real time action adapter
 * a table of type to create RtCommand
 */
public interface TableActionAdapter {

    RtCommand createUpdateRtCommand(CanalEntry.RowData rowData);

    RtCommand createInsertRtCommand(List<CanalEntry.Column> list);

    RtCommand createDeleteRtCommand(List<CanalEntry.Column> list);

}
