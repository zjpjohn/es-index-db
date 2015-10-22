package com.wxingyl.es.handle;

import com.wxingyl.es.db.DbTableDesc;
import com.wxingyl.es.index.IndexTypeBean;
import com.wxingyl.es.canal.ChangeDataEntry;

import java.util.List;
import java.util.Map;

/**
 * Created by xing on 15/9/24.
 * index type real time index action to deal data change
 */
public interface RtIndexHandle {

    /**
     * Note: if return Empty or null, will user {@link IndexTypeBean#getAllTableQueryInfo()}
     * @param instance canal instance name
     * @return support tables, if return Empty, will user {@link IndexTypeBean#getAllTableQueryInfo()},
     *          return null, don't support
     */
    List<DbTableDesc> supportTable(String instance);

    IndexTypeBean supportType(String instance);

    /**
     * the param tableGroupData.value is List<ChangeDataEntry>, it can modify
     * @param instance canal instance name
     * @param tableGroupData change data, key: schema.table, value: change data
     */
    void dealDataChange(String instance, Map<DbTableDesc, List<ChangeDataEntry>> tableGroupData);

}
