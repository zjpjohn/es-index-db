package com.wxingyl.es.canal;

import com.wxingyl.es.conf.ConfigManager;
import com.wxingyl.es.db.DbTableDesc;
import com.wxingyl.es.index.IndexTypeDesc;

import java.util.List;

/**
 * Created by xing on 15/9/25.
 * real time index config manager
 */
public interface RtIndexConfigManager extends ConfigManager {

    /**
     * @return fields list is unmodifiable
     */
    List<String> getTableFields(IndexTypeDesc type, DbTableDesc table);

    void addTableFields(IndexTypeDesc type, DbTableDesc table, List<String> fields);
}
