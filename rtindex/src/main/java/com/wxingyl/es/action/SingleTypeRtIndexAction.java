package com.wxingyl.es.action;

import com.wxingyl.es.canal.ChangeDataEntry;
import com.wxingyl.es.db.DbTableDesc;
import com.wxingyl.es.index.IndexTypeBean;
import com.wxingyl.es.util.CommonUtils;

import java.util.*;

/**
 * Created by xing on 15/9/29.
 * SingleTypeDeployRtIndexAction, this action only have one index/type, but it may have more than one canal instances
 */
public class SingleTypeRtIndexAction implements SingleTypeModifiableRtIndexAction {

    private IndexTypeBean type;

    /**
     * key: canal instance, value: support tables
     */
    protected Map<String, List<DbTableDesc>> canalTableMap = new HashMap<>();

    protected Map<DbTableDesc, TableAction> tableActionMap = new HashMap<>();

    public SingleTypeRtIndexAction(IndexTypeBean type) {
        this.type = type;
    }

    @Override
    public void registerTable(String instance, List<DbTableDesc> tables) {
        if (CommonUtils.isEmpty(tables)) throw new IllegalArgumentException("tables can not empty");
        List<DbTableDesc> list = canalTableMap.get(instance);
        if (list == null) {
            canalTableMap.put(instance, list = new ArrayList<>());
        }
        list.addAll(tables);
    }

    @Override
    public void registerTable(String instance) {
        canalTableMap.put(instance, Collections.<DbTableDesc>emptyList());
    }

    @Override
    public List<DbTableDesc> supportTable(String instance) {
        List<DbTableDesc> list = canalTableMap.get(instance);
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
        tableActionMap.put(table, action);
    }

    @Override
    public void dealDataChange(String instance, Map<DbTableDesc, List<ChangeDataEntry>> tableGroupData) {
        for (DbTableDesc table : tableGroupData.keySet()) {
            TableAction action = tableActionMap.get(table);
            if (action == null) {
                throw new IllegalStateException("type: " + type.getType() + ", table: " + table
                        + " don't set TableAction object");
            }
            action.createCommand(type.getType(), tableGroupData.get(table));
        }
    }
}
