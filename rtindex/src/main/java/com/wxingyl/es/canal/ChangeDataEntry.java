package com.wxingyl.es.canal;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.wxingyl.es.db.DbTableDesc;
import com.wxingyl.es.util.CommonUtils;
import com.wxingyl.es.util.EsUtils;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by xing on 15/9/28.
 * canal a row data change entry
 */
public class ChangeDataEntry {

    private DbTableDesc table;

    private CanalEntry.EventType eventType;

    private List<CanalEntry.RowData> rowData = new LinkedList<>();

    public ChangeDataEntry(DbTableDesc table, CanalEntry.EventType eventType) {
        this.eventType = eventType;
        this.table = table;
    }

    public void setRowData(List<CanalEntry.RowData> rowData) {
        this.rowData = rowData;
    }

    public void addRowData(List<CanalEntry.RowData> rowData) {
        this.rowData.addAll(rowData);
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
