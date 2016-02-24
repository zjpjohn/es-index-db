package com.wxingyl.es.action.adapter;

import com.wxingyl.es.action.TableAction;
import com.wxingyl.es.db.DbTableDesc;
import com.wxingyl.es.db.TableBaseInfo;
import com.wxingyl.es.db.query.QueryCondition;
import com.wxingyl.es.index.IndexManager;
import com.wxingyl.es.index.IndexTypeBean;
import com.wxingyl.es.index.IndexTypeDesc;
import com.wxingyl.es.index.db.SqlQueryCommon;
import com.wxingyl.es.util.CommonUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by xing on 15/10/19.
 * index type info, contain every table info in this index/type
 */
public class IndexTypeInfo {

    public static final String OBJ_DOCUMENT_PARENT_FIELD_KEY = "-1";

    private IndexTypeBean typeBean;

    private IndexManager indexManager;

    private String typeDocIdField;

    private Map<DbTableDesc, TableInfo> tableInfoMap = new HashMap<>();

    public IndexTypeInfo(IndexManager indexManager, String typeDocIdField, IndexTypeBean typeBean) {
        this.indexManager = indexManager;
        this.typeDocIdField = typeDocIdField;
        this.typeBean = typeBean;
    }

    public TableInfo addTableInfo(TableAction tableAction, Map<String, String> dbDocFieldMap) {
        TableInfo info = new TableInfo(tableAction, dbDocFieldMap);
        tableInfoMap.put(tableAction.getTable(), info);
        return info;
    }

    public TableInfo getTableInfo(DbTableDesc table) {
        return tableInfoMap.get(table);
    }

    public class TableInfo {

        //cache variable, for db_key_field
        private final String keyField;

        private final TableAction tableAction;

        private final boolean isMasterTable;

        private final Map<String, String> dbDocFieldMap;

        private TableActionAdapter actionAdapter;

        public TableInfo(TableAction tableAction, Map<String, String> dbDocFieldMap) {
            this.tableAction = tableAction;
            this.dbDocFieldMap = new HashMap<>();
            this.dbDocFieldMap.putAll(dbDocFieldMap);
            DbTableDesc table = tableAction.getTable();
            TableBaseInfo baseInfo = typeBean.getTableQueryInfo(table).getBaseInfo();
            this.keyField = baseInfo.getKeyField();
            if (baseInfo.getMasterAlias() != null && this.dbDocFieldMap.get(OBJ_DOCUMENT_PARENT_FIELD_KEY) == null) {
                this.dbDocFieldMap.put(OBJ_DOCUMENT_PARENT_FIELD_KEY, baseInfo.getMasterAlias());
            }
            tableAction.addTypeTableInfo(this);
            this.isMasterTable = table.equals(typeBean.getMasterTable().getQueryCommon().getBaseInfo().getTable());
        }

        public String getParentDocField() {
            return dbDocFieldMap.get(OBJ_DOCUMENT_PARENT_FIELD_KEY);
        }

        public String getDocField(String column) {
            return CommonUtils.getOrDefault(dbDocFieldMap, column, column);
        }

        public String getDocKeyField() {
            return CommonUtils.getOrDefault(dbDocFieldMap, keyField, keyField);
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

        public String getDocIdField() {
            return typeDocIdField;
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

        public DbTableDesc getTable() {
            return tableAction.getTable();
        }

        public Integer getKeyFieldIndex() {
            return tableAction.getColumnIndex(keyField);
        }

        public SqlQueryCommon getSqlQueryInfo() {
            return typeBean.getTableQueryInfo(tableAction.getTable());
        }

        void setActionAdapter(TableActionAdapter actionAdapter) {
            this.actionAdapter = actionAdapter;
        }

        public TableActionAdapter getActionAdapter() {
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
