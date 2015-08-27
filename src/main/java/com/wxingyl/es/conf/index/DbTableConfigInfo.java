package com.wxingyl.es.conf.index;

import com.wxingyl.es.exception.IndexConfigException;
import com.wxingyl.es.jdal.DbTableDesc;
import com.wxingyl.es.jdal.DbTableFieldDesc;
import com.wxingyl.es.util.CommonUtils;
import org.elasticsearch.common.collect.Tuple;

import java.util.List;
import java.util.Set;

import static com.wxingyl.es.conf.ConfigKeyName.*;

/**
 * Created by xing on 15/8/26.
 * parse config file, get table info
 */
public class DbTableConfigInfo {

    private DbTableDesc table;

    String dbAddress;

    // null is all fields
    Set<String> fields;

    Set<String> forbidFields;

    String deleteField;

    String deleteValidValue;
    // v1: table v2: field
    private DbTableFieldDesc masterField;

    String relationField;

    public void setTable(String schema, String tableName) {
        table = DbTableDesc.build(schema, tableName);
    }

    public void setMasterField(DbTableFieldDesc masterField) {
        this.masterField = masterField;
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

    void setDefaultValue(String key, String val) {
        switch (key) {
            case INDEX_DB_ADDRESS:
                dbAddress = val;
                break;
            case INDEX_DELETE_FIELD:
                deleteField = val;
                break;
            case INDEX_DELETE_VALID_VALUE:
                deleteValidValue = val;
                break;
        }
    }

    void formatFields() {
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
