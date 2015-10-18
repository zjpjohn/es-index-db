package com.wxingyl.es.action;

import com.wxingyl.es.db.DbTableDesc;
import com.wxingyl.es.index.IndexTypeBean;
import com.wxingyl.es.index.IndexTypeDesc;

import java.util.List;

/**
 * Created by xing on 15/9/29.
 *
 */
public interface MultiTypeModifiableRtIndexAction extends RtIndexAction {

    void registerTable(String instance, IndexTypeBean typeBean, List<DbTableDesc> tables);

    void registerTable(String instance, IndexTypeBean typeBean);

    void setTableAction(IndexTypeDesc type, DbTableDesc table, TableAction action);
}
