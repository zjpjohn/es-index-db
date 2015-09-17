package com.wxingyl.es.index.db;

import com.wxingyl.es.db.DbTableDesc;
import com.wxingyl.es.db.TableBaseInfo;
import com.wxingyl.es.db.query.QueryCondition;
import com.wxingyl.es.index.IndexSlaveResultMergeEnum;
import com.wxingyl.es.conf.index.DbTableConfigInfo;

import java.util.Set;

/**
 * Created by xing on 15/8/27.
 * query data from table, the sql has common part, eg:
 * SELECT XX, YY, ZZ FROM TABLE_NAME
 * so we can prepare create common part, no need to create every query
 */
public class SqlQueryCommon {
    /**
     * query table
     */
    private DbTableDesc table;

    /**
     * the field is primary key, its value should be unique in table
     */
    private String keyField;

    private String commonSql;

    private boolean containWhere;

    private int pageSize;

    private String orderBy;

    private String masterAlias;

    private TableBaseInfo tableBaseInfo;

    private IndexSlaveResultMergeEnum mergeType;

    private Set<QueryCondition> conditions;

    public String getOrderBy() {
        return orderBy;
    }

    public int getPageSize() {
        return pageSize;
    }

    public String getCommonSql() {
        return commonSql;
    }

    public boolean isContainWhere() {
        return containWhere;
    }

    public DbTableDesc getTable() {
        return table;
    }

    public TableBaseInfo getTableBaseInfo() {
        return tableBaseInfo == null ? tableBaseInfo = TableBaseInfo.build(this) : tableBaseInfo;
    }

    public String getMasterAlias() {
        return masterAlias;
    }

    public IndexSlaveResultMergeEnum getMergeType() {
        return mergeType;
    }

    public String getKeyField() {
        return keyField;
    }

    public Set<QueryCondition> getConditions() {
        return conditions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SqlQueryCommon)) return false;

        SqlQueryCommon that = (SqlQueryCommon) o;

        return table.equals(that.table);

    }

    @Override
    public int hashCode() {
        return table.hashCode();
    }

    @Override
    public String toString() {
        return "PrepareSqlQuery{" +
                "commonSql='" + commonSql + '\'' +
                ", orderBy='" + orderBy + '\'' +
                ", pageSize=" + pageSize +
                ", tableField=" + table +
                '}';
    }

    public static Build build() {
        return new Build();
    }

    public static class Build {

        private String commonSql;

        private boolean containWhere;

        private String orderBy;

        private Set<QueryCondition> conditions;

        public Build commonSql(String commonSql) {
            this.commonSql = commonSql;
            return this;
        }

        public Build containWhere() {
            this.containWhere = true;
            return this;
        }

        public Build orderBy(String orderBy) {
            this.orderBy = orderBy;
            return this;
        }

        public Build conditions(Set<QueryCondition> conditions) {
            this.conditions = conditions;
            return this;
        }

        public SqlQueryCommon build(DbTableConfigInfo tableInfo) {
            SqlQueryCommon sqlQueryCommon = new SqlQueryCommon();
            sqlQueryCommon.commonSql = commonSql;
            sqlQueryCommon.containWhere = containWhere;
            sqlQueryCommon.orderBy = orderBy;
            sqlQueryCommon.pageSize = tableInfo.getPageSize();
            sqlQueryCommon.table = tableInfo.getTable();
            sqlQueryCommon.keyField = tableInfo.getRelationField();
            sqlQueryCommon.masterAlias = tableInfo.getMasterAlias();
            sqlQueryCommon.mergeType = tableInfo.getMergeType();
            sqlQueryCommon.conditions = conditions;
            return sqlQueryCommon;
        }
    }
}
