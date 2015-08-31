package com.wxingyl.es.jdal.handle;

import com.wxingyl.es.conf.index.DbTableConfigInfo;
import com.wxingyl.es.jdal.*;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.handlers.MapListHandler;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * Created by xing on 15/8/26.
 * sql query handler: build query sql, run query
 */
public interface SqlQueryHandle {

    FilterMapListHandler DEFAULT_MAP_LIST_HANDLER = new FilterMapListHandler(null);

    PrepareSqlQuery createPrepareSqlQuery(DbTableConfigInfo tableInfo);

    DbQueryResult query(SqlQueryParam param) throws SQLException;

    /**
     * get all tables in the schema
     */
    Set<String> getAllTables(String schema) throws ExecutionException;

    /**
     * get all fields in the schema.table
     */
    Set<String> getAllFields(DbTableDesc table) throws ExecutionException;

}
