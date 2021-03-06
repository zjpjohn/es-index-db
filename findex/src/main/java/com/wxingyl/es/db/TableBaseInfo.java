package com.wxingyl.es.db;

import com.wxingyl.es.index.db.SqlQueryCommon;
import com.wxingyl.es.index.IndexSlaveResultMergeEnum;

/**
 * Created by xing on 15/9/7.
 * table query result base info, don't have query result and param.pageSize
 */
public class TableBaseInfo {

    private DbTableDesc table;

    /**
     * the field is primary key, its value should be unique in table
     */
    private String keyField;
    /**
     * when {@link #mergeType} is {@link IndexSlaveResultMergeEnum#MERGE}, masterAlias is prefix name of field which is have
     * conflict
     */
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

    protected void init(SqlQueryCommon sqlQueryCommon) {
        table = sqlQueryCommon.getTable();
        keyField = sqlQueryCommon.getKeyField();
        masterAlias = sqlQueryCommon.getMasterAlias();
        mergeType = sqlQueryCommon.getMergeType();
    }

    protected void init(TableBaseInfo other) {
        table = other.table;
        keyField = other.keyField;
        masterAlias = other.masterAlias;
        mergeType = other.mergeType;
    }

    public static TableBaseInfo build(SqlQueryCommon sqlQueryCommon) {
        TableBaseInfo baseInfo = new TableBaseInfo();
        baseInfo.init(sqlQueryCommon);
        return baseInfo;
    }
}
