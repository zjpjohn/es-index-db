package com.wxingyl.es.action;

import com.wxingyl.es.db.DbTableDesc;

import java.util.List;

/**
 * Created by xing on 15/9/29.
 * this action only have a index/type
 */
public interface SingleTypeDeployRtIndexAction extends RtIndexAction {

    void registerTableAction(String instance, List<DbTableDesc> tables);

    /**
     * register all table of this type for instance
     */
    void registerTableAction(String instance);
}
