package com.wxingyl.es.action;

import com.wxingyl.es.canal.ChangeDataEntry;
import com.wxingyl.es.db.DbTableDesc;
import com.wxingyl.es.index.IndexTypeBean;
import com.wxingyl.es.index.IndexTypeDesc;
import com.wxingyl.es.util.CommonUtils;

import java.util.*;

/**
 * Created by xing on 15/9/29.
 * MultiTypeRtIndexAction, In this Action, a canal instance only one index/type,
 * if canal instance need more than one index/type, you should create other RtIndexAction object
 */
public class MultiTypeRtIndexAction implements MultiTypeModifiableRtIndexAction {

    /**
     * In an action, a canal instance should only one index/type
     * key: canal instance name, value: index/type entry
     */
    protected Map<String, TypeEntry> canalTypeMap = new HashMap<>();

    protected Map<IndexTypeDesc, Map<DbTableDesc, TableAction>> typeTableActionMap = new HashMap<>();

    @Override
    public List<DbTableDesc> supportTable(String instance) {
        TypeEntry entry = canalTypeMap.get(instance);
        if (entry == null) return null;
        else if (entry.supportTable.isEmpty()) return Collections.emptyList();
        else return Collections.unmodifiableList(entry.supportTable);
    }

    @Override
    public IndexTypeBean supportType(String instance) {
        TypeEntry entry = canalTypeMap.get(instance);
        return entry == null ? null : entry.typeBean;
    }

    @Override
    public void registerTable(String instance, IndexTypeBean typeBean, List<DbTableDesc> tables) {
        if (CommonUtils.isEmpty(tables)) throw new IllegalArgumentException("tables can not empty");
        addTableAction(instance, typeBean, tables);
    }

    @Override
    public void registerTable(String instance, IndexTypeBean typeBean) {
        addTableAction(instance, typeBean, Collections.<DbTableDesc>emptyList());
    }

    @Override
    public void setTableAction(IndexTypeDesc type, DbTableDesc table, TableAction action) {
        Map<DbTableDesc, TableAction> map = typeTableActionMap.get(type);
        if (map == null) {
            typeTableActionMap.put(type, map = new HashMap<>());
        }
        map.put(table, action);
    }

    @Override
    public void dealDataChange(String instance, Map<DbTableDesc, List<ChangeDataEntry>> tableGroupData) {
        IndexTypeDesc typeDesc = canalTypeMap.get(instance).typeBean.getType();
        Map<DbTableDesc, TableAction> map = typeTableActionMap.get(typeDesc);
        if (map == null) {
            throw new IllegalStateException("type: " + typeDesc + " don't set TableAction object");
        }
        for (DbTableDesc table : tableGroupData.keySet()) {
            TableAction action = map.get(table);
            if (action == null) {
                throw new IllegalStateException("type: " + typeDesc + ", table: " + table + " don't set TableAction object");
            }
            action.createCommand(typeDesc, tableGroupData.get(table));
        }
    }

    private void addTableAction(String instance, IndexTypeBean typeBean, List<DbTableDesc> tables) {
        TypeEntry entry = canalTypeMap.get(instance);
        if (!(entry == null || entry.typeBean.getType().equals(typeBean.getType()))) {
            throw new IllegalArgumentException("tables can not empty");
        }
        if (entry == null) {
            canalTypeMap.put(instance, entry = new TypeEntry(instance, typeBean));
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
