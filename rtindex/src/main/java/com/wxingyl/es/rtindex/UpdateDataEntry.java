package com.wxingyl.es.rtindex;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.wxingyl.es.db.DbTableDesc;

import java.util.List;

/**
 * Created by xing on 15/9/28.
 */
public class UpdateDataEntry extends ChangeDataEntry {

    public UpdateDataEntry(DbTableDesc table, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowData) {
        super(table, eventType, rowData);
    }

}
