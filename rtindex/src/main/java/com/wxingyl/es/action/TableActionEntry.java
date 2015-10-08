package com.wxingyl.es.action;

import com.wxingyl.es.canal.ChangeDataEntry;
import com.wxingyl.es.db.DbTableDesc;
import com.wxingyl.es.db.query.QueryCondition;
import com.wxingyl.es.index.IndexTypeBean;
import com.wxingyl.es.index.db.SqlQueryCommon;

import java.util.*;

/**
 * Created by xing on 15/9/30.
 * table action entry
 */
public class TableActionEntry {

    private DbTableDesc table;
    /**
     * unmodifiableMap
     */
    private Map<String, QueryCondition> conditionMap;

    private TableAction action;

    private TableActionEntry() {
    }

    public TableAction getAction() {
        return action;
    }

    /**
     * unmodifiableMap
     */
    public Map<String, QueryCondition> getConditionMap() {
        return conditionMap;
    }

    public DbTableDesc getTable() {
        return table;
    }

    //TODO 创建任务，实时索引更新机制通过改变了什么砸门就修改啥的策略
    public void onAction(List<ChangeDataEntry> data) {

    }

    public static TableActionEntry build(IndexTypeBean type, DbTableDesc table, TableAction action) {
        TableActionEntry entry = new TableActionEntry();
        SqlQueryCommon common = type.getTableQueryInfo(table);
        Objects.requireNonNull(common);
        entry.action = action;
        entry.table = table;
        if (common.getConditions() != null) {
            Map<String, QueryCondition> map = new HashMap<>();
            for (QueryCondition qc : common.getConditions()) {
                map.put(qc.getField(), qc);
            }
            entry.conditionMap = Collections.unmodifiableMap(map);
        }
        return entry;
    }
}
