package com.wxingyl.es.action;

import com.wxingyl.es.canal.ChangeDataEntry;
import com.wxingyl.es.db.DbTableDesc;
import com.wxingyl.es.index.IndexTypeBean;
import com.wxingyl.es.index.IndexTypeDesc;
import com.wxingyl.es.util.CommonUtils;

import java.util.*;

/**
 * Created by xing on 15/9/29.
 * abstract implement class of TypeRtIndexAction
 */
public class MultiTypeRtIndexAction implements MultiTypeModifiableRtIndexAction {

    /**
     * In an action, a canal instance should only one index/type
     * key: canal instance name, value: index/type entry
     */
    protected Map<String, TypeEntry> actionMap = new HashMap<>();

    protected Map<IndexTypeDesc, Map<DbTableDesc, TableActionEntry>> tableActionEntryMap = new HashMap<>();

    @Override
    public List<DbTableDesc> supportTable(String instance) {
        TypeEntry entry = actionMap.get(instance);
        if (entry == null) return null;
        else if (entry.supportTable.isEmpty()) return Collections.emptyList();
        else return Collections.unmodifiableList(entry.supportTable);
    }

    @Override
    public IndexTypeBean supportType(String instance) {
        TypeEntry entry = actionMap.get(instance);
        return entry == null ? null : entry.typeBean;
    }

    @Override
    public void registerTableAction(String instance, IndexTypeBean typeBean, List<DbTableDesc> tables) {
        if (CommonUtils.isEmpty(tables)) throw new IllegalArgumentException("tables can not empty");
        addTableAction(instance, typeBean, tables);
    }

    @Override
    public void registerTableAction(String instance, IndexTypeBean typeBean) {
        addTableAction(instance, typeBean, Collections.<DbTableDesc>emptyList());
    }

    @Override
    public void setTableAction(IndexTypeBean typeBean, DbTableDesc table, TableAction action) {
        Map<DbTableDesc, TableActionEntry> map = tableActionEntryMap.get(typeBean.getType());
        if (map == null) {
            tableActionEntryMap.put(typeBean.getType(), map = new HashMap<>());
        }
        map.put(table, TableActionEntry.build(typeBean, table, action));
    }

    @Override
    public void dealDataChange(String instance, Map<DbTableDesc, List<ChangeDataEntry>> tableGroupData) {
        IndexTypeDesc typeDesc = actionMap.get(instance).typeBean.getType();
        Map<DbTableDesc, TableActionEntry> map = tableActionEntryMap.get(typeDesc);
        if (map == null) {
            throw new IllegalStateException("type: " + typeDesc + " don't set TableAction object");
        }
        for (DbTableDesc table : tableGroupData.keySet()) {
            TableActionEntry entry = map.get(table);
            if (entry == null) {
                throw new IllegalStateException("type: " + typeDesc + ", table: " + table + " don't set TableAction object");
            }
            entry.onAction(tableGroupData.get(table));
        }
    }

    private void addTableAction(String instance, IndexTypeBean typeBean, List<DbTableDesc> tables) {
        TypeEntry entry = actionMap.get(instance);
        if (!(entry == null || entry.typeBean.getType().equals(typeBean.getType()))) {
            throw new IllegalArgumentException("tables can not empty");
        }
        if (entry == null) {
            actionMap.put(instance, entry = new TypeEntry(instance, typeBean));
        }

        if (tables.isEmpty()) {
            entry.supportTable = tables;
        } else {
            if (entry.supportTable == null) {
                entry.supportTable = new ArrayList<>();
            }
            entry.supportTable.addAll(tables);
        }
    }

    protected static class TypeEntry {

        protected String instance;

        protected IndexTypeBean typeBean;

        protected List<DbTableDesc> supportTable;

        public TypeEntry(String instance, IndexTypeBean typeBean) {
            this.typeBean = typeBean;
            this.instance = instance;
        }
    }
}
