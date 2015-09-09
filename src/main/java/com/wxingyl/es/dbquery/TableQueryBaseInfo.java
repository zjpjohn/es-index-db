package com.wxingyl.es.dbquery;

import com.wxingyl.es.conf.IndexSlaveResultMergeEnum;

/**
 * Created by xing on 15/9/7.
 * table query result base info, don't have query result and param.pageSize
 */
public class TableQueryBaseInfo {

    private DbTableDesc table;

    /**
     * the field is primary key, its value should be unique in table
     */
    private String keyField;

    private String masterAlias;

    private IndexSlaveResultMergeEnum mergeType;

    public DbTableDesc getTable() {
        return table;
    }

    public String getKeyField() {
        return keyField;
    }

    public String getMasterAlias() {
        return masterAlias;
    }

    public IndexSlaveResultMergeEnum getMergeType() {
        return mergeType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TableQueryBaseInfo)) return false;

        TableQueryBaseInfo baseInfo = (TableQueryBaseInfo) o;

        return table.equals(baseInfo.table);
    }

    protected void init(SqlQueryCommon sqlQueryCommon) {
        table = sqlQueryCommon.getTable();
        keyField = sqlQueryCommon.getKeyField();
        masterAlias = sqlQueryCommon.getMasterAlias();
        mergeType = sqlQueryCommon.getMergeType();
    }

    protected void init(TableQueryBaseInfo other) {
        table = other.table;
        keyField = other.keyField;
        masterAlias = other.masterAlias;
        mergeType = other.mergeType;
    }

    @Override
    public int hashCode() {
        return table.hashCode();
    }

    public static TableQueryBaseInfo build(SqlQueryCommon sqlQueryCommon) {
        TableQueryBaseInfo baseInfo = new TableQueryBaseInfo();
        baseInfo.init(sqlQueryCommon);
        return baseInfo;
    }
}
