package com.wxingyl.es.conf.index;

import org.elasticsearch.common.collect.Tuple;

import java.util.List;
import java.util.Set;

import static com.wxingyl.es.conf.ConfigKeyName.*;

/**
 * Created by xing on 15/8/26.
 * parse config file, get table info
 */
public class DbTableConfigInfo {
    
    String schema;

    String dbAddress;

    String tableName;
    // null is all fields
    Set<String> fields;

    Set<String> forbidFields;

    String deleteField;

    String deleteValidValue;
    // v1: table v2: field
    Tuple<String, String> masterField;

    String relationField;

    public String getSchema() {
        return schema;
    }

    public String getDbAddress() {
        return dbAddress;
    }

    public String getTableName() {
        return tableName;
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

    public Tuple<String, String> getMasterField() {
        return masterField;
    }

    public String getRelationField() {
        return relationField;
    }

    void setDefaultValue(String key, String val) {
        switch (key) {
            case INDEX_SCHEMA:
                schema = val;
                break;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DbTableConfigInfo)) return false;

        DbTableConfigInfo that = (DbTableConfigInfo) o;

        if (!schema.equals(that.schema)) return false;
        if (dbAddress != null ? !dbAddress.equals(that.dbAddress) : that.dbAddress != null) return false;
        return tableName.equals(that.tableName);

    }

    @Override
    public int hashCode() {
        int result = schema.hashCode();
        result = 31 * result + (dbAddress != null ? dbAddress.hashCode() : 0);
        result = 31 * result + tableName.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return String.format("DbTableConfigInfo[schemaName: %s, dbAddress: %s, tableName: %s]", schema, dbAddress, tableName);
    }
}
