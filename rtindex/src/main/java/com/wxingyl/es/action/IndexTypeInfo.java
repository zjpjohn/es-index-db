package com.wxingyl.es.action;

import com.wxingyl.es.db.DbTableDesc;
import com.wxingyl.es.db.query.QueryCondition;
import com.wxingyl.es.index.IndexManager;
import com.wxingyl.es.index.IndexTypeBean;
import com.wxingyl.es.index.IndexTypeDesc;
import com.wxingyl.es.index.db.SqlQueryCommon;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by xing on 15/10/19.
 * index type info
 */
public class IndexTypeInfo {

    private IndexTypeBean typeBean;

    private IndexManager indexManager;

    private String idField;

    private Map<DbTableDesc, TableInfo> tableInfoMap = new HashMap<>();

    public IndexTypeInfo(IndexManager indexManager, String idField, IndexTypeBean typeBean) {
        this.indexManager = indexManager;
        this.idField = idField;
        this.typeBean = typeBean;
    }

    public TableInfo addTableInfo(TableAction tableAction) {
        TableInfo info = new TableInfo(tableAction);
        tableInfoMap.put(tableAction.getTable(), info);
        return info;
    }

    public TableInfo getTableInfo(DbTableDesc table) {
        return tableInfoMap.get(table);
    }

    public class TableInfo {

        //cache variable
        private String keyField;

        private TableAction tableAction;

        private TypeTableActionAdapter actionAdapter;

        private final boolean isMasterTable;

        public TableInfo(TableAction tableAction) {
            DbTableDesc table = tableAction.getTable();
            keyField = typeBean.getTableQueryInfo(table).getBaseInfo().getKeyField();
            this.tableAction = tableAction;
            tableAction.addTypeTableInfo(this);
            isMasterTable = table.equals(typeBean.getMasterTable().getQueryCommon().getBaseInfo().getTable());
        }

        public String getKeyField() {
            return keyField;
        }

        public Set<QueryCondition> getQueryCondition() {
            return typeBean.getTableQueryInfo(tableAction.getTable()).getConditions();
        }

        public IndexManager getIndexManager() {
            return indexManager;
        }

        public String getIdField() {
            return idField;
        }

        public IndexTypeBean getTypeBean() {
            return typeBean;
        }

        public IndexTypeDesc getType() {
            return typeBean.getType();
        }

        public TableAction getTableAction() {
            return tableAction;
        }

        public SqlQueryCommon getSqlQueryInfo() {
            return typeBean.getTableQueryInfo(tableAction.getTable());
        }

        void setActionAdapter(TypeTableActionAdapter actionAdapter) {
            this.actionAdapter = actionAdapter;
        }

        public TypeTableActionAdapter getActionAdapter() {
            return actionAdapter;
        }

        public boolean isMasterTable() {
            return isMasterTable;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof TableInfo)) return false;

            TableInfo tableInfo = (TableInfo) o;

            return (tableAction == tableInfo.tableAction || tableAction.getTable().equals(tableInfo.tableAction.getTable()))
                    && (typeBean == tableInfo.getTypeBean() || typeBean.getType().equals(tableInfo.getType()));
        }

        @Override
        public int hashCode() {
            return keyField != null ? keyField.hashCode() : 0;
        }
    }

}
