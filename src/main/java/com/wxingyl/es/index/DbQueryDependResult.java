package com.wxingyl.es.index;

import com.wxingyl.es.jdal.DbTableDesc;

/**
 * Created by xing on 15/9/2.
 *
 */
public class DbQueryDependResult {

    private DbTableDesc masterTable;

    private String keyField;

    public DbTableDesc getMasterTable() {
        return masterTable;
    }

    public String getKeyField() {
        return keyField;
    }
}
