package com.wxingyl.es.action;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.wxingyl.es.command.UpdateRtCommand;

/**
 * Created by xing on 15/10/20.
 * real time action adapter, we can freedom create RtCommand
 */
public interface TypeTableActionAdapt {

    void initTableAction(TableAction action);

    UpdateRtCommand createUpdateRtCommand(CanalEntry.RowData rowData);

}
