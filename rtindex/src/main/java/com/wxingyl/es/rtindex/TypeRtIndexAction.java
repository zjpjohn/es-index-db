package com.wxingyl.es.rtindex;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.wxingyl.es.db.DbTableDesc;
import com.wxingyl.es.index.IndexTypeBean;

import java.util.List;
import java.util.Map;

/**
 * Created by xing on 15/9/24.
 * index type real time index action to deal data change
 */
public interface TypeRtIndexAction {

    /**
     * Note: if return Empty or null, will user {@link IndexTypeBean#getAllTableInfo()}
     * @param instance canal instance name
     * @return support tables, if return Empty or null, will user {@link IndexTypeBean#getAllTableInfo()}
     */
    List<DbTableDesc> supportTable(String instance);

    IndexTypeBean supportType(String instance);

    /**
     * @param instance canal instance name
     * @param tableGroupData change data, key: schema.table
     */
    void dealDataChange(String instance, Map<String, List<CanalEntry.RowData>> tableGroupData);

}
