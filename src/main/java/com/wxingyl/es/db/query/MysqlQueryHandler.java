package com.wxingyl.es.db.query;

import com.wxingyl.es.conf.index.DbTableConfigInfo;
import com.wxingyl.es.db.DbTableDesc;
import com.wxingyl.es.index.db.SqlQueryCommon;
import com.wxingyl.es.db.result.TableQueryResult;
import com.wxingyl.es.index.db.SqlQueryParam;
import com.wxingyl.es.util.CommonUtils;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.search.sort.SortOrder;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.*;

/**
 * Created by xing on 15/8/26.
 * mysql query default handler
 */
public class MysqlQueryHandler extends AbstractSqlQueryHandler {

    public MysqlQueryHandler(DataSource dataSource) {
        super(dataSource, new MysqlQueryStatementStructure());
    }

    @Override
    protected String createSql(BaseQueryParam param) {
        StringBuilder sb = new StringBuilder();
        appendSelectSql(sb, param.getFields(), param.getTable());
        if (param.getConditions() != null) {
            sb.append(" WHERE ");
            param.getConditions().forEach(c -> c.appendQuerySql(sb, queryStatementStructure));
        }
        if (param.getOrderBy() != null) {
            appendOrderBySql(sb, param.getOrderBy());
        }
        if (param.getPage() != null) {
            appendPageSql(sb, param.getPage());
        }
        return sb.toString();
    }

    private void appendPageSql(StringBuilder sb, Tuple<Integer, Integer> page) {
        sb.append(" LIMIT ").append(page.v1()).append(", ").append(page.v2());
    }

    private void appendOrderBySql(StringBuilder sb, Map<String, SortOrder> order) {
        sb.append(" ORDER BY ");
        order.forEach((k, v) -> {
            sb.append('`').append(k).append("`");
            if (v != null) {
                sb.append(' ').append(v);
            }
            sb.append(", ");
        });
        sb.delete(sb.length() - 2, sb.length());
    }

    private void appendSelectSql(StringBuilder sb, Set<String> fields, DbTableDesc table) {
        sb.append("SELECT ");
        if (CommonUtils.isEmpty(fields)) {
            sb.append('*');
        } else {
            fields.forEach(f -> {
                queryStatementStructure.appendField(sb, f).append(", ");
            });
            sb.delete(sb.length() - 2, sb.length());
        }
        sb.append(" FROM ").append(schemaTableSql(table));
    }

    @Override
    protected Set<String> loadAllTables(String schema) throws Exception {
        List<Map<String, Object>> result = getQueryRunner().query("SHOW TABLES IN `" + schema + '`', DEFAULT_MAP_LIST_HANDLER);
        final Set<String> tables = new HashSet<>();
        result.forEach(m -> m.values().forEach(v -> tables.add(v.toString())));
        return tables;
    }

    @Override
    protected Set<String> loadAllFields(DbTableDesc table) throws Exception {
        List<Map<String, Object>> result = getQueryRunner().query("SHOW COLUMNS FROM " + schemaTableSql(table), DEFAULT_MAP_LIST_HANDLER);
        final Set<String> fields = new HashSet<>();
        result.forEach(m -> fields.add(m.get("field").toString()));
        return fields;
    }

    private String schemaTableSql(DbTableDesc table) {
        return '`' + table.getSchema() + "`.`" + table.getTable() + '`';
    }

    @Override
    public SqlQueryCommon createPrepareSqlQuery(DbTableConfigInfo tableInfo) {
        StringBuilder sb = new StringBuilder();
        appendSelectSql(sb, tableInfo.getFields(), tableInfo.getTable());
        SqlQueryCommon.Build build = SqlQueryCommon.build();
        Set<QueryCondition> conditions = tableInfo.getQueryConditions();
        if (conditions != null) {
            sb.append(" WHERE ");
            conditions.forEach(c -> {
                c.appendQuerySql(sb, queryStatementStructure);
                sb.append(" AND ");
            });
            sb.delete(sb.length() - 5, sb.length());
            build.containWhere();
        }
        return build.commonFormatSql(sb.toString())
                .orderBy("ORDER BY `" + tableInfo.getRelationField() + '`')
                .build(tableInfo);
    }

    @Override
    public TableQueryResult query(SqlQueryParam param) throws SQLException {
        SqlQueryCommon prepareSql = param.getQueryCommon();
        StringBuilder sb = new StringBuilder(prepareSql.getCommonSql());
        if (param.getQueryCondition() != null) {
            if (prepareSql.isContainWhere()) sb.append(" AND ");
            else sb.append(" WHERE ");
            param.getQueryCondition().appendQuerySql(sb, queryStatementStructure);
        }
        sb.append(' ').append(prepareSql.getOrderBy());
        appendPageSql(sb, Tuple.tuple(param.getPage() * prepareSql.getPageSize(), prepareSql.getPageSize()));
        return TableQueryResult.build().dbData(getQueryRunner().query(sb.toString(), param.getRsh())).build(prepareSql);
    }

}
