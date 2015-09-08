package com.wxingyl.es.conf;

import com.wxingyl.es.conf.ds.DataSourceBean;
import com.wxingyl.es.conf.ds.DataSourceConfigParse;
import com.wxingyl.es.jdal.DbTableDesc;

import java.util.Map;

/**
 * Created by xing on 15/9/8.
 * config manager
 */
public interface ConfigManager {

    boolean addDataSourceConfigParser(DataSourceConfigParse parser);

    boolean supportDbParse(String driverClassName);

    DataSourceBean getDataSourceBean(DbTableDesc table);

    void parseDataSource(String yamlFileName);

    void parseDataSource(Map<String, Object> confMap);

    void parseIndexType(String yamlFileName);

    void parseIndexType(Map<String, Object> confMap);
}
