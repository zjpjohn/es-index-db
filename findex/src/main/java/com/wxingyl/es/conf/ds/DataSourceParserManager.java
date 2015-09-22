package com.wxingyl.es.conf.ds;

import com.wxingyl.es.conf.ConfigParse;
import com.wxingyl.es.db.DataSourceBean;

import java.util.Map;
import java.util.Set;

/**
 * Created by xing on 15/9/8.
 * data source parser manager
 */
public interface DataSourceParserManager extends ConfigParse<DataSourceBean> {

    /**
     * 是否支持该数据源配置解析
     * @return true 支持 false 不支持
     */
    boolean supportParse(String driverClassName);

    boolean addDataSourceConfigParser(DataSourceConfigParse parser);

    Set<DataSourceBean> parseAll(Map<String, Map<String, Object>> configMap);
}
