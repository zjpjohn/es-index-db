package com.wxingyl.es.conf.index;

import com.wxingyl.es.exception.IndexConfigException;
import com.wxingyl.es.jdal.DbTableDesc;
import com.wxingyl.es.jdal.DbTableFieldDesc;
import com.wxingyl.es.util.CommonUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.wxingyl.es.conf.ConfigKeyName.*;

/**
 * Created by xing on 15/8/26.
 * parse config file, get table info
 */
public class DbTableConfigInfo {

    private DbTableDesc table;

    private String dbAddress;

    // null is all fields
    private Set<String> fields;

    private Set<String> forbidFields;

    private String deleteField;

    private String deleteValidValue;

    private DbTableFieldDesc masterField;

    private String masterAlias;

    private String relationField;

    private Integer pageSize;

    public void setMasterField(DbTableFieldDesc masterField) {
        if (masterField.newDbTableDesc().equals(table)) {
            throw new IndexConfigException("Index type config: " + toString() + ", " + INDEX_TABLE_MASTER_FIELD
                    + " value can't local table");
        }
        this.masterField = masterField;
        if (masterAlias == null) {
            masterAlias = masterField.getField() + "_info";
        } else {
            masterAlias = masterAlias.toLowerCase();
        }
    }

    public String getSchema() {
        return table.getSchema();
    }

    public String getDbAddress() {
        return dbAddress;
    }

    public String getTableName() {
        return table.getTable();
    }

    public DbTableDesc getTable() {
        return table;
    }

    public Set<String> getFields() {
        return fields;
    }

    public Set<String> getForbidFields() {
        return forbidFields;
    }

    public String getDeleteField() {
        return deleteField;
    }

    public void clearDeleteField() {
        deleteField = null;
        deleteValidValue = null;
    }

    public String getDeleteValidValue() {
        return deleteValidValue;
    }

    public DbTableFieldDesc getMasterField() {
        return masterField;
    }

    public String getRelationField() {
        return relationField;
    }

    public Integer getPageSize() {
        return pageSize;
    }

    public String getMasterAlias() {
        return masterAlias;
    }

    private void setDefaultValue(String key, String val) {
        switch (key) {
            case INDEX_TABLE_DB_ADDRESS:
                dbAddress = val;
                break;
            case INDEX_TABLE_DELETE_FIELD:
                deleteField = val.toLowerCase();
                break;
            case INDEX_TABLE_DELETE_VALID_VALUE:
                deleteValidValue = val;
                break;
        }
    }

    void initValue(TypeConfigInfo typeInfo, Map<String, Object> conf, Map<String, String> defaultVal) {
        String tableName = CommonUtils.getStringVal(conf, INDEX_TABLE_TABLE);
        if (tableName == null) {
            throw new IndexConfigException("table_name conf is null of " + typeInfo);
        }
        table = DbTableDesc.build(defaultVal.remove(INDEX_TABLE_SCHEMA), tableName);
        if (table.getSchema() == null) {
            throw new IndexConfigException(typeInfo + " conf, table_name: " + tableName + " can't find schema");
        }
        defaultVal.forEach(this::setDefaultValue);
        if (deleteField != null && deleteValidValue == null) {
            throw new IndexConfigException(typeInfo + " conf, table_name: " + tableName + " delete_field: "
                    + deleteField + ", but delete_valid_value is null");
        }
        relationField = CommonUtils.getStringVal(conf, INDEX_TABLE_RELATION_FIELD);
        if (relationField == null) {
            throw new IndexConfigException(typeInfo + ", table_name: " + tableName
                    + " need " + INDEX_TABLE_RELATION_FIELD + " config");
        } else {
            relationField = relationField.toLowerCase();
        }
        pageSize = (Integer) conf.getOrDefault(INDEX_TABLE_PAGE_SIZE, 2500);
        masterAlias = CommonUtils.getStringVal(conf, INDEX_TABLE_MASTER_ALIAS);
        forbidFields = getFields(conf, INDEX_TABLE_FORBID_FIELDS);
        fields = getFields(conf, INDEX_TABLE_FIELDS);
        if (CommonUtils.isEmpty(fields) || fields.contains("*")) {
            fields = null;
        } else if (!CommonUtils.isEmpty(forbidFields)) {
            fields.removeAll(forbidFields);
            if (fields.isEmpty()) {
                throw new IndexConfigException(toString() + " fields retain forbid_fields is empty, forbid_fields: "
                        + forbidFields);
            }
            //fields is not empty, so forbidFields it useless
            forbidFields = null;
        }
        if (fields != null && !fields.contains(relationField)) {
            fields.add(relationField);
        }
        if (forbidFields != null && forbidFields.contains(relationField)) {
            forbidFields.remove(relationField);
        }
    }

    private Set<String> getFields(Map<String, Object> map, String key) {
        List<String> list = CommonUtils.getList(map, key);
        if (CommonUtils.isEmpty(list)) return null;
        Set<String> set = new HashSet<>();
        for (String s : list) {
            set.add(s.toLowerCase());
        }
        return set;
    }

    void addFiled(String filed) {
        if (fields != null && !fields.contains(filed)) {
            fields.add(filed);
        }
        if (forbidFields != null && forbidFields.contains(filed)) {
            forbidFields.remove(filed);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DbTableConfigInfo)) return false;

        DbTableConfigInfo that = (DbTableConfigInfo) o;

        if (dbAddress != null ? !dbAddress.equals(that.dbAddress) : that.dbAddress != null) return false;
        return table.equals(that.table);

    }

    @Override
    public int hashCode() {
        int result = table.hashCode();
        result = 31 * result + (dbAddress != null ? dbAddress.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return String.format("DbTableConfigInfo[schemaName: %s, dbAddress: %s, tableName: %s]", table.getSchema(),
                dbAddress, table.getTable());
    }

}
