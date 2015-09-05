package com.wxingyl.es.conf.index;

import com.wxingyl.es.exception.IndexConfigException;
import com.wxingyl.es.jdal.DbTableDesc;
import com.wxingyl.es.jdal.DbTableFieldDesc;
import com.wxingyl.es.util.CommonUtils;
import com.wxingyl.es.util.DefaultValueParser;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static com.wxingyl.es.conf.ConfigKeyName.*;

/**
 * Created by xing on 15/8/26.
 * parse config file, get table info
 */
public class DbTableConfigInfo {

    private DbTableDesc table;

    // null is all fields
    private Set<String> fields;

    private Set<String> forbidFields;

    private String deleteField;

    private String deleteValidValue;

    private DbTableFieldDesc masterField;

    private String masterAlias;

    private String relationField;

    private Integer pageSize;

    private String queryCondition;

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

    public String getQueryCondition() {
        return queryCondition;
    }

    void initValue(TypeConfigInfo typeInfo, Map<String, Object> conf,
                   DefaultValueParser<String> strDvp, DefaultValueParser<Integer> intDvp) {
        String tableName = CommonUtils.getStringVal(conf, INDEX_TABLE_TABLE);
        if (tableName == null) {
            throw new IndexConfigException("table_name conf is null of " + typeInfo);
        }
        String[] strArray = new String[2];
        strDvp.getDefaultValue(conf).forEach((k, v) -> {
            switch (k) {
                case INDEX_TABLE_SCHEMA:
                    strArray[0] = v;
                    break;
                case INDEX_TABLE_DB_ADDRESS:
                    strArray[1] = v;
                    break;
                case INDEX_TABLE_DELETE_FIELD:
                    deleteField = v == null ? null : v.toLowerCase();
                    break;
                case INDEX_TABLE_DELETE_VALID_VALUE:
                    deleteValidValue = v;
                    break;
            }
        });
        if (deleteField != null && deleteValidValue == null) {
            throw new IndexConfigException(typeInfo + " conf, table_name: " + tableName + " delete_field: "
                    + deleteField + ", but delete_valid_value is null");
        }
        if (strArray[0] == null) {
            throw new IndexConfigException(typeInfo + " conf, table_name: " + tableName + " can't find schema");
        }
        relationField = CommonUtils.getStringVal(conf, INDEX_TABLE_RELATION_FIELD);
        if (relationField == null) {
            throw new IndexConfigException(typeInfo + ", table_name: " + tableName
                    + " need " + INDEX_TABLE_RELATION_FIELD + " config");
        } else {
            relationField = relationField.toLowerCase();
        }
        table = new DbTableDesc(strArray[1], strArray[0], tableName);
        pageSize = intDvp.getDefaultValue(conf).getOrDefault(INDEX_TABLE_PAGE_SIZE, 2500);
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
        queryCondition = CommonUtils.getStringVal(conf, INDEX_TABLE_QUERY_CONDITION);
    }

    private Set<String> getFields(Map<String, Object> map, String key) {
        List<String> list = CommonUtils.getList(map, key);
        if (CommonUtils.isEmpty(list)) return null;
        return list.stream().collect(HashSet::new, (c, s) -> c.add(s.toLowerCase()), Set::addAll);
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

        return table.equals(that.table);
    }

    @Override
    public int hashCode() {
        return table.hashCode();
    }

    @Override
    public String toString() {
        return String.format("DbTableConfigInfo[%s]", table);
    }

}
