package com.wxingyl.es.jdal.handle;

import com.wxingyl.es.conf.index.DbTableConfigInfo;
import com.wxingyl.es.jdal.DbQueryResult;
import com.wxingyl.es.jdal.DbTableDesc;
import com.wxingyl.es.jdal.PrepareSqlQuery;
import com.wxingyl.es.jdal.SqlQueryParam;
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
    protected Set<String> loadAllFields(DbTableDesc table) throws Exception {
        return null;
    }

    @Override
    public PrepareSqlQuery createPrepareSqlQuery(DbTableConfigInfo tableInfo) {
        return null;
    }

    @Override
    public DbQueryResult query(SqlQueryParam param) throws SQLException {
        return null;
    }

}
