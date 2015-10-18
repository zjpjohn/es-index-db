package com.wxingyl.es.action;

import com.wxingyl.es.db.DbTableDesc;

import java.util.List;

/**
 * Created by xing on 15/9/29.
 * this action only have a index/type
 */
public interface SingleTypeModifiableRtIndexAction extends RtIndexAction {

    void registerTable(String instance, List<DbTableDesc> tables);

    /**
     * register all table of this type for instance
     */
    void registerTable(String instance);

    void setTableAction(DbTableDesc table, TableAction action);
}
