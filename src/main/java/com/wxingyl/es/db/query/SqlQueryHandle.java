package com.wxingyl.es.db.query;

import com.wxingyl.es.conf.index.DbTableConfigInfo;
import com.wxingyl.es.db.DbTableDesc;
import com.wxingyl.es.db.SqlQueryCommon;
import com.wxingyl.es.db.result.TableQueryResult;
import org.apache.commons.dbutils.ResultSetHandler;

import java.sql.SQLException;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * Created by xing on 15/8/26.
 * sql query handler: build query sql, run query
 */
public interface SqlQueryHandle {

    SqlQueryCommon createPrepareSqlQuery(DbTableConfigInfo tableInfo);

    TableQueryResult query(SqlQueryParam param) throws SQLException;

    <T> T query(BaseQueryParam param, ResultSetHandler<T> rsh) throws SQLException;
    /**
     * get all tables in the schema
     */
    Set<String> getAllTables(String schema) throws ExecutionException;

    /**
     * get all fields in the schema.table
     */
    Set<String> getAllFields(DbTableDesc table) throws ExecutionException;

}
