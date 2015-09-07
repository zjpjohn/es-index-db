package com.wxingyl.es.conf.ds;

import com.wxingyl.es.conf.ConfigKeyName;
import com.wxingyl.es.exception.DataSourceConfigException;
import com.wxingyl.es.util.CommonUtils;

import java.util.*;

/**
 * Created by xing on 15/8/17.
 * data source parse factory
 * parse config file by driver_class_name, it can select suitable parser to get db info
 */
public class DataSourceParseFactory implements DataSourceConfigParse {

    private Set<DataSourceConfigParse> parserSet = new HashSet<>();

    @SuppressWarnings("unchecked")
    @Override
    public Set<DataSourceBean> parse(Map<String, Object> yamlConf) {
        final String driverClassName = CommonUtils.getStringVal(yamlConf, ConfigKeyName.DS_DRIVER_CLASS_NAME);
        if (driverClassName == null) {
            throw new DataSourceConfigException("dataSource config of driver_class_name is empty");
        }
        List<Map<String, Object>> schemaList = CommonUtils.getList(yamlConf, ConfigKeyName.DS_SCHEMA_LIST);
        if (schemaList == null) {
            throw new DataSourceConfigException("dataSource config don't have " + ConfigKeyName.DS_SCHEMA_LIST);
        }
        DataSourceConfigParse parse = null;
        for (DataSourceConfigParse e : parserSet) {
            if (e.supportParse(driverClassName)) {
                parse = e;
                break;
            }
        }
        if (parse == null) {
            throw new DataSourceConfigException("There is not a support parser for driver_class_name: " + driverClassName);
        }
        return parse.parse(schemaList);
    }

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
    public boolean removeDataSourceConfigParser(DataSourceConfigParse parser) {
        return parserSet.remove(parser);
    }
}
