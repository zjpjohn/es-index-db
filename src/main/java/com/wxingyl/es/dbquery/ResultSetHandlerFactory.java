package com.wxingyl.es.dbquery;

import com.wxingyl.es.conf.ConfigManager;
import com.wxingyl.es.conf.index.DbTableConfigInfo;
import org.apache.commons.dbutils.ResultSetHandler;

import java.util.List;
import java.util.Map;

/**
 * Created by xing on 15/9/9.
 * db query resultSet handler factory
 */
public interface ResultSetHandlerFactory {

    ResultSetHandler<List<Map<String, Object>>> get(ConfigManager configManager, DbTableConfigInfo tableInfo);

}
