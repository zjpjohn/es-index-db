package com.wxingyl.es.handle;

import com.wxingyl.es.action.TableAction;
import com.wxingyl.es.db.DbTableDesc;

import java.util.List;

/**
 * Created by xing on 15/9/29.
 * this action only have a index/type
 */
public interface SingleTypeModifiableRtIndexHandle extends RtIndexHandle {

    void registerTable(String instance, List<DbTableDesc> tables);

    /**
     * register all table of this type for instance
     */
    void registerTable(String instance);

    void setTableAction(DbTableDesc table, TableAction action);
}
