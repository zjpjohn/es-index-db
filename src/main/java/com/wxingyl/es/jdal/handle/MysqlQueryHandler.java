package com.wxingyl.es.jdal.handle;

import com.wxingyl.es.conf.index.DbTableConfigInfo;
import com.wxingyl.es.jdal.DbQueryResult;
import com.wxingyl.es.jdal.DbTableDesc;
import com.wxingyl.es.jdal.PrepareSqlQuery;
import com.wxingyl.es.jdal.SqlQueryParam;
import com.wxingyl.es.util.CommonUtils;

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
    protected Set<String> loadAllFields(DbTableDesc table) throws Exception {
        List<Map<String, Object>> result = queryRunner.query("SHOW COLUMNS FROM " + table.getSchema() + '.'
                + table.getTable(), DEFAULT_MAP_LIST_HANDLER);
        final Set<String> fields = new HashSet<>();
        result.forEach(m -> fields.add(m.get("field").toString()));
        return fields;
    }

    @Override
    public PrepareSqlQuery createPrepareSqlQuery(DbTableConfigInfo tableInfo) {
        final StringBuilder sb = new StringBuilder();
        sb.append("SELECT ");
        if (tableInfo.getFields() != null) {
            for (String s : tableInfo.getFields()) {
                sb.append(s).append(", ");
            }
            sb.delete(sb.length() - 2, sb.length());
        } else {
            sb.append('*');
        }
        sb.append(" FROM ").append(tableInfo.getSchema()).append('.').append(tableInfo.getTableName());
        PrepareSqlQuery.Build build = PrepareSqlQuery.build();
        if (tableInfo.getDeleteField() != null) {
            sb.append(" WHERE ").append(tableInfo.getDeleteField()).append(" = ").append('\'')
                    .append(tableInfo.getDeleteValidValue()).append('\'');
            build.containWhere();
        }

        return build.commonFormatSql(sb.toString())
                .orderBy("ORDER BY " + tableInfo.getRelationField())
                .build(tableInfo);
    }

    @Override
    public DbQueryResult query(SqlQueryParam param) throws SQLException {
        PrepareSqlQuery prepareSql = param.getPrepareSql();
        StringBuilder sb = new StringBuilder(prepareSql.getCommonFormatSql());
        if (!CommonUtils.isEmpty(param.getKeyValueList())) {
            if (prepareSql.isContainWhere()) sb.append(" AND");
            sb.append(' ').append(prepareSql.getKeyField()).append(" IN (");
            sb.append(')');
        }
        sb.append(' ').append(prepareSql.getOrderBy()).append(" LIMIT ").append(param.getPage() * prepareSql.getPageSize())
                .append(", ").append(prepareSql.getPageSize());
        return new DbQueryResult(prepareSql, queryRunner.query(sb.toString(), param.getRsh()));
    }

}
