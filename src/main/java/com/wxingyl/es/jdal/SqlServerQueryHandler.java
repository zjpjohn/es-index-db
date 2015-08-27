package com.wxingyl.es.jdal;

import com.wxingyl.es.conf.index.DbTableConfigInfo;
import org.apache.commons.dbutils.ResultSetHandler;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by xing on 15/8/27.
 * sql server query default handler
 */
public class SqlServerQueryHandler extends AbstractSqlQueryHandler {

    public SqlServerQueryHandler(DataSource dataSource) {
        super(dataSource);
    }

    @Override
    protected Set<String> loadAllTables(String schema) throws Exception {
        return null;
    }

    @Override
    protected Set<String> loadAllFields(String schema, String table) throws Exception {
        return null;
    }

    @Override
    public PrepareSqlQuery createPrepareSqlQuery(DbTableConfigInfo tableInfo) {
        return null;
    }

    @Override
    public List<Map<String, Object>> query(SqlQueryParam param, ResultSetHandler<List<Map<String, Object>>> handler) throws SQLException {
        return null;
    }
}
