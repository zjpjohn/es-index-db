package com.wxingyl.es.conf;

import com.wxingyl.es.conf.ds.DataSourceConfigParse;
import com.wxingyl.es.db.DataSourceBean;
import com.wxingyl.es.index.IndexTypeBean;
import com.wxingyl.es.index.IndexTypeDesc;
import com.wxingyl.es.util.Listener;

import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

/**
 * Created by xing on 15/9/8.
 * config manager
 * should singleton obj
 * In the first place, we should parse datasource config ({@link #parseDataSource(String)}, {@link #parseDataSource(String, Map)}),
 * then parse index config ({@link #parseIndexType(String)}, {@link #parseIndexType(String, Map)}). If you index config had included
 * datasource config, this order is needless
 */
public interface ConfigManager {

    boolean addDataSourceConfigParser(DataSourceConfigParse parser);

    boolean supportDbParse(String driverClassName);

    boolean registerDataSourceListener(Listener<Set<DataSourceBean>> listener);

    boolean registerIndexTypeListener(Listener<Set<IndexTypeBean>> listener);

    DataSourceBean findDataSourceBean(String schema, String urlAddress);

    IndexTypeBean findIndexTypeBean(IndexTypeDesc type);

    SortedSet<IndexTypeBean> findIndexTypeBean(String index);

    /**
     * parse data_source yaml config file, it have more than one type db
     */
    void parseDataSource(String yamlFileName);

    /**
     * parse one type db config
     */
    void parseDataSource(String configName, Map<String, Object> confMap);

    void parseIndexType(String yamlFileName);

    void parseIndexType(String index, Map<String, Object> confMap);
}
