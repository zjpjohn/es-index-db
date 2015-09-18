package com.wxingyl.es.conf.ds;

import com.wxingyl.es.conf.ConfigKeyName;
import com.wxingyl.es.db.DataSourceBean;
import com.wxingyl.es.exception.DataSourceConfigException;
import com.wxingyl.es.util.CommonUtils;

import java.util.*;

/**
 * Created by xing on 15/8/17.
 * data source parse factory
 * parse config file by driver_class_name, it can select suitable parser to get db info
 */
public class DataSourceParseFactory implements DataSourceParserManager {

    private Set<DataSourceConfigParse> parserSet = new HashSet<>();

    @Override
    public boolean supportParse(String driverClassName) {
        for (DataSourceConfigParse e : parserSet) {
            if (e.supportParse(driverClassName)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean addDataSourceConfigParser(DataSourceConfigParse parser) {
        return parserSet.add(parser);
    }

    @Override
    public Set<DataSourceBean> parseAll(Map<String, Map<String, Object>> configMap) {
        Set<DataSourceBean> set = new HashSet<>();
        for (Map.Entry<String, Map<String, Object>> e : configMap.entrySet()) {
            Set<DataSourceBean> parseRet = parse(e.getKey(), e.getValue());
            if (!CommonUtils.isEmpty(parseRet)) set.addAll(parseRet);
        }
        return set;
    }

    @Override
    public Set<DataSourceBean> parse(String configName, Map<String, Object> config) {
        String driverClassName = CommonUtils.getStringVal(config, ConfigKeyName.DS_DRIVER_CLASS_NAME);
        if (driverClassName == null) {
            throw new DataSourceConfigException("dataSource config: " + configName + " of driver_class_name is empty");
        }
        List<Map<String, Object>> schemaList = CommonUtils.getList(config, ConfigKeyName.DS_SCHEMA_LIST);
        if (schemaList == null) {
            throw new DataSourceConfigException("dataSource config: " + configName + " don't have " + ConfigKeyName.DS_SCHEMA_LIST);
        }
        DataSourceConfigParse parse = null;
        for (DataSourceConfigParse e : parserSet) {
            if (e.supportParse(driverClassName)) {
                parse = e;
                break;
            }
        }
        if (parse == null) {
            throw new DataSourceConfigException("There is not a support parser for driver_class_name: "
                    + driverClassName + " in " + configName);
        }
        return parse.parseSchemas(configName, schemaList);
    }
}
