package com.wxingyl.es.rtindex;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.wxingyl.es.db.DbTableDesc;

import java.util.List;

/**
 * Created by xing on 15/9/28.
 * canal a row data change entry
 */
public class ChangeDataEntry {

    private DbTableDesc table;

    private CanalEntry.EventType eventType;

    private List<CanalEntry.RowData> rowData;

    public ChangeDataEntry(DbTableDesc table, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowData) {
        this.eventType = eventType;
        this.table = table;
        this.rowData = rowData;
    }

    public CanalEntry.EventType getEventType() {
        return eventType;
    }

    public List<CanalEntry.RowData> getRowData() {
        return rowData;
    }

    public DbTableDesc getTable() {
        return table;
    }
}
