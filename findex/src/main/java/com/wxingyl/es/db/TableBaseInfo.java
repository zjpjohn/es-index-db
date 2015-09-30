package com.wxingyl.es.db;

import com.wxingyl.es.conf.index.DbTableConfigInfo;
import com.wxingyl.es.index.db.SqlQueryCommon;
import com.wxingyl.es.index.IndexSlaveResultMergeEnum;

/**
 * Created by xing on 15/9/7.
 * table query result base info, don't have query result and param.pageSize
 */
public class TableBaseInfo {

    protected DbTableDesc table;

    /**
     * the field is primary key, its value should be unique in table
     */
    protected String keyField;
    /**
     * when {@link #mergeType} is {@link IndexSlaveResultMergeEnum#MERGE}, masterAlias is prefix name of field which is have
     * conflict
     */
    protected String masterAlias;

    protected IndexSlaveResultMergeEnum mergeType;

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

    public static TableBaseInfo build(DbTableConfigInfo tableInfo) {
        TableBaseInfo baseInfo = new TableBaseInfo();
        baseInfo.table = tableInfo.getTable();
        baseInfo.keyField = tableInfo.getRelationField();
        baseInfo.masterAlias = tableInfo.getMasterAlias();
        baseInfo.mergeType = tableInfo.getMergeType();
        return baseInfo;
    }
}
