package com.wxingyl.es.action;

import com.alibaba.otter.canal.protocol.CanalEntry;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by xing on 15/10/16.
 * table column index
 */
public class TableColumnIndex {

    private Map<String, Integer> columnIndex = new HashMap<>();

    public void reload(List<CanalEntry.Column> data) {
        columnIndex.clear();
        for (int i = 0; i < data.size(); i++) {
            columnIndex.put(data.get(i).getName(), i);
        }
    }

    public Integer getIndex(String column) {
        return columnIndex.get(column);
    }

    public int columnNum() {
        return columnIndex.size();
    }

}
