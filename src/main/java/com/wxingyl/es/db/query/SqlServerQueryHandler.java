package com.wxingyl.es.db.query;

import com.wxingyl.es.conf.index.DbTableConfigInfo;
import com.wxingyl.es.db.DbTableDesc;
import com.wxingyl.es.index.db.SqlQueryCommon;
import com.wxingyl.es.db.result.TableQueryResult;
import com.wxingyl.es.index.db.SqlQueryParam;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Set;

/**
 * Created by xing on 15/8/27.
 * sql server query default handler
 * TODO: all function has not implement
 */
public class SqlServerQueryHandler extends AbstractSqlQueryHandler {

    public SqlServerQueryHandler(DataSource dataSource) {
        super(dataSource, null);
    }

    @Override
    protected String createCountSql(SqlQueryCommon common) {
        return null;
    }

    @Override
    protected String createSql(BaseQueryParam param) {
        return null;
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
    public SqlQueryCommon createPrepareSqlQuery(DbTableConfigInfo tableInfo) {
        return null;
    }

    @Override
    public TableQueryResult query(SqlQueryParam param) throws SQLException {
        return null;
    }

}
