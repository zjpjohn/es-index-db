package com.wxingyl.es.conf.ds;

import com.wxingyl.es.conf.ConfigParse;
import com.wxingyl.es.db.DataSourceBean;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by xing on 15/8/17.
 * data source config parse
 */
public interface DataSourceConfigParse extends ConfigParse<DataSourceBean> {

    /**
     * 是否支持该数据源配置解析
     * @return true 支持 false 不支持
     */
    boolean supportParse(String driverClassName);

    /**
     * a type database parse
     * @param name define database name
     * @param schemaList this database contain schema list
     * @return parse result
     */
    Set<DataSourceBean> parseSchemas(String name, List<Map<String, Object>> schemaList);

}
