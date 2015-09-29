package com.wxingyl.es.action;

import com.wxingyl.es.db.DbTableDesc;
import com.wxingyl.es.index.IndexTypeBean;

import java.util.List;

/**
 * Created by xing on 15/9/29.
 *
 */
public interface DeployRtIndexAction extends RtIndexAction {

    void registerTableAction(String instance, IndexTypeBean typeBean, List<DbTableDesc> tables);

    void registerTableAction(String instance, IndexTypeBean typeBean);
}
