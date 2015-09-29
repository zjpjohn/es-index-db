package com.wxingyl.es.action;

import com.wxingyl.es.db.DbTableDesc;
import com.wxingyl.es.index.IndexTypeBean;
import com.wxingyl.es.rtindex.ChangeDataEntry;

import java.util.List;
import java.util.Map;

/**
 * Created by xing on 15/9/24.
 * index type real time index action to deal data change
 */
public interface RtIndexAction {

    /**
     * Note: if return Empty or null, will user {@link IndexTypeBean#getAllTableInfo()}
     * @param instance canal instance name
     * @return support tables, if return Empty, will user {@link IndexTypeBean#getAllTableInfo()},
     *          return null, don't support
     */
    List<DbTableDesc> supportTable(String instance);

    IndexTypeBean supportType(String instance);

    /**
     * @param instance canal instance name
     * @param tableGroupData change data, key: schema.table
     */
    void dealDataChange(String instance, Map<DbTableDesc, List<ChangeDataEntry>> tableGroupData);

}
