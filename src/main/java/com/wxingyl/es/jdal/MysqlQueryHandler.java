package com.wxingyl.es.jdal;

import com.wxingyl.es.conf.index.DbTableConfigInfo;
import org.apache.commons.dbutils.ResultSetHandler;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by xing on 15/8/26.
 * mysql query default handler
 */
public class MysqlQueryHandler extends AbstractSqlQueryHandler {

    public MysqlQueryHandler(DataSource dataSource) {
        super(dataSource);
    }

    @Override
    protected Set<String> loadAllTables(String schema) throws Exception {
        List<Map<String, Object>> result = queryRunner.query("SHOW TABLES IN " + schema, DEFAULT_MAP_LIST_HANDLER);
        final Set<String> tables = new HashSet<>();
        result.forEach(m -> m.values().forEach(v -> tables.add(v.toString())));
        return tables;
    }

    @Override
    protected Set<String> loadAllFields(String schema, String table) throws Exception {
        List<Map<String, Object>> result = queryRunner.query("SHOW COLUMNS FROM " + schema + '.' + table, DEFAULT_MAP_LIST_HANDLER);
        final Set<String> fields = new HashSet<>();
        result.forEach(m -> fields.add(m.get("Field").toString()));
        return fields;
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
