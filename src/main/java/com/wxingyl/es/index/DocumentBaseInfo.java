package com.wxingyl.es.index;

import com.wxingyl.es.jdal.DbTableDesc;
import com.wxingyl.es.jdal.TableQueryResult;

/**
 * Created by xing on 15/9/7.
 * document base info, such as index, type, table-name and so on
 */
public class DocumentBaseInfo {

    private DbTableDesc table;

    private IndexTypeDesc type;

    /**
     * the field is primary key, its value should be unique in table
     */
    private String keyField;

    private String masterAlias;

    private DocumentBaseInfo() {}

    public DbTableDesc getTable() {
        return table;
    }

    public IndexTypeDesc getType() {
        return type;
    }

    public String getKeyField() {
        return keyField;
    }

    public String getMasterAlias() {
        return masterAlias;
    }

    public static DocumentBaseInfo build(TableQueryResult tableResult, IndexTypeDesc type) {
        DocumentBaseInfo baseInfo = new DocumentBaseInfo();
        baseInfo.type = type;
        baseInfo.masterAlias = tableResult.getMasterAlias();
        baseInfo.table = tableResult.getTable();
        baseInfo.keyField = tableResult.getKeyField();
        return baseInfo;
    }
}
