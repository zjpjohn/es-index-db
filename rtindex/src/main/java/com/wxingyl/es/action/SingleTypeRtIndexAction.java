package com.wxingyl.es.action;

import com.wxingyl.es.canal.ChangeDataEntry;
import com.wxingyl.es.db.DbTableDesc;
import com.wxingyl.es.index.IndexTypeBean;
import com.wxingyl.es.util.CommonUtils;

import java.util.*;

/**
 * Created by xing on 15/9/29.
 * SingleTypeDeployRtIndexAction abstract implement
 */
public class SingleTypeRtIndexAction implements SingleTypeModifiableRtIndexAction {

    private IndexTypeBean type;

    /**
     * key: canal instance, value: support tables
     */
    protected Map<String, List<DbTableDesc>> tableActionMap = new HashMap<>();

    protected Map<DbTableDesc, TableActionEntry> tableActionEntryMap = new HashMap<>();

    public SingleTypeRtIndexAction(IndexTypeBean type) {
        this.type = type;
    }

    @Override
    public void registerTableAction(String instance, List<DbTableDesc> tables) {
        if (CommonUtils.isEmpty(tables)) throw new IllegalArgumentException("tables can not empty");
        List<DbTableDesc> list = tableActionMap.get(instance);
        if (list == null) {
            tableActionMap.put(instance, list = new ArrayList<>());
        }
        list.addAll(tables);
    }

    @Override
    public void registerTableAction(String instance) {
        tableActionMap.put(instance, Collections.<DbTableDesc>emptyList());
    }

    @Override
    public List<DbTableDesc> supportTable(String instance) {
        List<DbTableDesc> list = tableActionMap.get(instance);
        if (list == null) return null;
        else if (list.isEmpty()) return list;
        else return Collections.unmodifiableList(list);
    }

    @Override
    public IndexTypeBean supportType(String instance) {
        return type;
    }

    @Override
    public void setTableAction(DbTableDesc table, TableAction action) {
        tableActionEntryMap.put(table, TableActionEntry.build(type, table, action));
    }

    @Override
    public void dealDataChange(String instance, Map<DbTableDesc, List<ChangeDataEntry>> tableGroupData) {
        for (DbTableDesc table : tableGroupData.keySet()) {
            TableActionEntry entry = tableActionEntryMap.get(table);
            if (entry == null) {
                throw new IllegalStateException("type: " + type.getType() + ", table: " + table + " don't set TableAction object");
            }
            entry.onAction(tableGroupData.get(table));
        }
    }
}
