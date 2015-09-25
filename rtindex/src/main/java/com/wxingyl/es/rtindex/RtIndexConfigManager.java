package com.wxingyl.es.rtindex;

import com.wxingyl.es.conf.ConfigManager;
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
    List<String> getTableFields(IndexTypeDesc type, String table);
}
