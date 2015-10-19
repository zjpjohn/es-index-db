package com.wxingyl.es.action;

import com.wxingyl.es.command.ChangedFieldEntry;
import com.wxingyl.es.command.UpdateRtCommand;
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

    public TableInfo getTableInfo(DbTableDesc table) {
        TableInfo ret = tableInfoMap.get(table);
        if (ret == null) {
            tableInfoMap.put(table, ret = initTableInfo(table));
        }
        return ret;
    }

    protected TableInfo initTableInfo(DbTableDesc table) {
        TableInfo ret = new TableInfo();
        ret.keyField = type.getTableQueryInfo(table).getBaseInfo().getKeyField();
        ret.queryCondition = type.getTableQueryInfo(table).getConditions();
        return ret;
    }

    public class TableInfo {

        private String keyField;

        private Set<QueryCondition> queryCondition;

        //TODO need implement, docField and dbField
        private Map<String, String> docFieldMap = new HashMap<>();

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

        public String getDocField(String dbField) {
            return docFieldMap.get(dbField);
        }

        public void addChangedFieldEntry(String dbField, String beforeVal, String afterValue, UpdateRtCommand rtCommand) {
            //TODO ChangedFieldEntry.setIsQueryCondition()
            rtCommand.addChangeField(new ChangedFieldEntry(docFieldMap.get(dbField), beforeVal, afterValue));
        }
    }

}
