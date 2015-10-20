package com.wxingyl.es.action;

import com.wxingyl.es.db.DbTableDesc;
import com.wxingyl.es.db.query.QueryCondition;
import com.wxingyl.es.index.IndexTypeBean;
import com.wxingyl.es.index.IndexTypeDesc;
import org.elasticsearch.client.Client;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by xing on 15/10/19.
 * index type info
 */
public class IndexTypeInfo {

    private IndexTypeBean type;

    private Client client;

    private String idField;

    private Map<DbTableDesc, TableInfo> tableInfoMap = new HashMap<>();

    public IndexTypeInfo(Client client, String idField, IndexTypeBean type) {
        this.client = client;
        this.idField = idField;
        this.type = type;
    }

    public void addTableInfo(TableInfo tableInfo) {
        tableInfoMap.put(tableInfo.getTable(), tableInfo);
    }

    public TableInfo addTableInfo(DbTableDesc table, Map<String, String> dbDocFieldMap) {
        TableInfo info = new TableInfo(table, dbDocFieldMap);
        tableInfoMap.put(table, info);
        return info;
    }

    public TableInfo addTableInfo(DbTableDesc table, TypeTableActionAdapt typeActionAdapt) {
        TableInfo info = new TableInfo(table, typeActionAdapt);
        tableInfoMap.put(table, info);
        return info;
    }

    public TableInfo getTableInfo(DbTableDesc table) {
        return tableInfoMap.get(table);
    }

    public class TableInfo {

        private DbTableDesc table;

        private String keyField;

        private Set<QueryCondition> queryCondition;

        private TypeTableActionAdapt typeActionAdapt;

        public TableInfo(DbTableDesc table, Map<String, String> dbDocFieldMap) {
            this(table, new DefaultTypeTableActionAdapter(type.getType(), dbDocFieldMap));
        }

        public TableInfo(DbTableDesc table, TypeTableActionAdapt typeActionAdapt) {
            this.table = table;
            keyField = type.getTableQueryInfo(table).getBaseInfo().getKeyField();
            queryCondition = type.getTableQueryInfo(table).getConditions();
            this.typeActionAdapt = typeActionAdapt;
        }

        public String getKeyField() {
            return keyField;
        }

        public Set<QueryCondition> getQueryCondition() {
            return queryCondition;
        }

        public Client getClient() {
            return client;
        }

        public String getIdField() {
            return idField;
        }

        public IndexTypeDesc getType() {
            return type.getType();
        }

        public TypeTableActionAdapt getTypeActionAdapt() {
            return typeActionAdapt;
        }

        public DbTableDesc getTable() {
            return table;
        }
    }

}
