package com.wxingyl.es.jdal.handle;

import com.wxingyl.es.conf.index.DbTableConfigInfo;
import com.wxingyl.es.jdal.TableQueryResult;
import com.wxingyl.es.jdal.DbTableDesc;
import com.wxingyl.es.jdal.SqlQueryCommon;
import com.wxingyl.es.jdal.SqlQueryParam;

import javax.sql.DataSource;
import java.sql.SQLException;
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
    public SqlQueryCommon createPrepareSqlQuery(DbTableConfigInfo tableInfo) {
        return null;
    }

    @Override
    public TableQueryResult query(SqlQueryParam param) throws SQLException {
        return null;
    }

}
