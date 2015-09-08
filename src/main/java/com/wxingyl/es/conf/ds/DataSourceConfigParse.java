package com.wxingyl.es.conf.ds;

import com.wxingyl.es.conf.ConfigParse;
import com.wxingyl.es.util.CommonUtils;
import org.elasticsearch.common.collect.Sets;

import java.util.Map;
import java.util.Set;

/**
 * Created by xing on 15/8/17.
 * 数据源配置解析
 */
public interface DataSourceConfigParse extends ConfigParse<DataSourceBean> {

    /**
     * 是否支持该数据源配置解析
     * @return true 支持 false 不支持
     */
    boolean supportParse(String driverClassName);

    boolean addDataSourceConfigParser(DataSourceConfigParse parser);

    default Set<DataSourceBean> parse(Iterable<Map<String, Object>> schemaListConf) {
        Set<DataSourceBean> set = Sets.newHashSet();
        schemaListConf.forEach(v -> CommonUtils.addAll(set, parse(v)));
        return set;
    }

}
