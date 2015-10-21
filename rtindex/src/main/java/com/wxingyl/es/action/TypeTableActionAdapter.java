package com.wxingyl.es.action;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.wxingyl.es.command.delete.DeleteRtCommand;
import com.wxingyl.es.command.insert.InsertRtCommand;
import com.wxingyl.es.command.update.UpdateRtCommand;

import java.util.List;

/**
 * Created by xing on 15/10/20.
 * real time action adapter, we can freedom create RtCommand
 */
public interface TypeTableActionAdapter {

    UpdateRtCommand createUpdateRtCommand(CanalEntry.RowData rowData);

    InsertRtCommand createInsertRtCommand(List<CanalEntry.Column> list);

    DeleteRtCommand createDeleteRtCommand(List<CanalEntry.Column> list);

}
