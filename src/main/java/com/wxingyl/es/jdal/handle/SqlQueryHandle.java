package com.wxingyl.es.jdal.handle;

import com.wxingyl.es.conf.index.DbTableConfigInfo;
import com.wxingyl.es.jdal.DbQueryResult;
import com.wxingyl.es.jdal.PrepareSqlQuery;
import com.wxingyl.es.jdal.SqlQueryParam;
import org.apache.commons.dbutils.ResultSetHandler;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by xing on 15/8/26.
 * sql query handler: build query sql, run query
 */
public interface SqlQueryHandle {

    PrepareSqlQuery createPrepareSqlQuery(DbTableConfigInfo tableInfo);

    DbQueryResult query(SqlQueryParam param) throws SQLException;

    /**
     * get all tables in the schema
     */
    Set<String> getAllTables(String schema) throws Exception;

    /**
     * get all fields in the schema.table
     */
    Set<String> getAllFields(String schema, String table) throws Exception;

}
