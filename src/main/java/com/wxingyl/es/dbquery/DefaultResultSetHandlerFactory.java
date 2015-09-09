package com.wxingyl.es.dbquery;

import com.wxingyl.es.conf.ConfigManager;
import com.wxingyl.es.conf.index.DbTableConfigInfo;
import com.wxingyl.es.util.CommonUtils;
import org.apache.commons.dbutils.ResultSetHandler;

import java.util.List;
import java.util.Map;

/**
 * Created by xing on 15/9/9.
 * db query resultSet handler default factory
 */
public class DefaultResultSetHandlerFactory implements ResultSetHandlerFactory {

    public static final DefaultResultSetHandlerFactory INSTANCE = new DefaultResultSetHandlerFactory();

    private ResultSetHandler<List<Map<String, Object>>> defaultFilterMapListHandler;

    private FieldValueProcessor defaultFieldValueProcessor;

    private DefaultResultSetHandlerFactory() {
    }

    @Override
    public ResultSetHandler<List<Map<String, Object>>> get(ConfigManager configManager, DbTableConfigInfo tableInfo) {
        if (defaultFieldValueProcessor == null) {
            defaultFieldValueProcessor = new NumberFieldValueProcessor(configManager);
        }
        if (defaultFilterMapListHandler == null) {
            defaultFilterMapListHandler = new FilterMapListHandler(null, defaultFieldValueProcessor);
        }
        if (CommonUtils.isEmpty(tableInfo.getForbidFields())) {
            return defaultFilterMapListHandler;
        } else {
            return new FilterMapListHandler(tableInfo.getForbidFields(), defaultFieldValueProcessor);
        }
    }

}
