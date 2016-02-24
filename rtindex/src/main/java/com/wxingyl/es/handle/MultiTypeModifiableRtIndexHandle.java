package com.wxingyl.es.handle;

import com.wxingyl.es.action.TableAction;
import com.wxingyl.es.db.DbTableDesc;
import com.wxingyl.es.index.IndexTypeBean;
import com.wxingyl.es.index.IndexTypeDesc;

import java.util.List;

/**
 * Created by xing on 15/9/29.
 * multi type RtIndexHandle and is modifiable
 */
public interface MultiTypeModifiableRtIndexHandle extends RtIndexHandle {

    void registerTable(String instance, IndexTypeBean typeBean, List<DbTableDesc> tables);

    void registerTable(String instance, IndexTypeBean typeBean);

    void setTableAction(IndexTypeDesc type, DbTableDesc table, TableAction action);
}
